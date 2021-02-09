#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <natsmd/ev_nats_client.h>
#include <raikv/ev_publish.h>
#include <raikv/kv_pubsub.h>
#include <raimd/md_types.h>

using namespace rai;
using namespace natsmd;
using namespace kv;
using namespace md;

EvNatsClient::EvNatsClient( EvPoll &p ) noexcept
    : EvConnection( p, p.register_type( "natsclient" ) ),
      protocol( 1 ), name( 0 ), lang( 0 ), version( 0 ),
      user( 0 ), pass( 0 ), auth_token( 0 )
{
}

EvNatsClient *
EvNatsClient::create_nats_client( EvPoll &p ) noexcept
{
  void * m = aligned_malloc( sizeof( EvNatsClient ) );
  if ( m == NULL ) {
    perror( "alloc nats" );
    return NULL;
  }
  return new ( m ) EvNatsClient( p );
}

bool
EvNatsClient::connect( const char *host,  int port ) noexcept
{
  if ( this->fd != -1 )
    return false;
  this->initialize_state();
  if ( EvTcpConnection::connect( *this, host, port,
                                 DEFAULT_TCP_CONNECT_OPTS ) != 0 ) {
    fprintf( stderr, "create NATS socket failed\n" );
    return false;
  }
  return true;
}

void
EvNatsClient::do_shutdown( void ) noexcept
{
  if ( this->fd != -1 )
    this->idle_push( EV_SHUTDOWN );
}

void
EvNatsClient::process( void ) noexcept
{
  enum { DO_OK = 1, DO_ERR = 2, NEED_MORE = 4, FLOW_BACKPRESSURE = 8,
         HAS_PING = 16 };
  static const char pong[] = "PONG\r\n";
  size_t            buflen, used, sz, nargs, size_len;
  char            * p, * eol, * start, * end, * size_start;
  NatsArgs          args;
  int               fl;

  for (;;) {
    buflen = this->len - this->off;
    if ( buflen == 0 )
      goto break_loop;

    start = &this->recv[ this->off ];
    end   = &start[ buflen ];
    used  = 0;
    fl    = 0;
    /* decode nats hdrs */
    for ( p = start; p < end; ) {
      if ( this->msg_state == NATS_HDR_STATE ) {
        eol = (char *) ::memchr( &p[ 1 ], '\n', end - &p[ 1 ] );
        if ( eol != NULL ) {
          sz = &eol[ 1 ] - p;
          if ( sz > 3 ) {
            switch ( unaligned<uint32_t>( p ) & 0xdfdfdfdf ) { /* 4 toupper */
              case NATS_KW_OK1:     // client side only
              case NATS_KW_OK2:     break;

              case NATS_KW_MSG1:   // MSG <subject> <sid> [reply] <size>
              case NATS_KW_MSG2:
                size_start = args.parse_end_size( p, eol - 1, this->msg_len,
                                                  size_len );
                if ( size_start == NULL ) {
                  fl |= DO_ERR;
                  break;
                }
                if ( sz <= this->tmp_size ) {
                  ::memcpy( this->buffer, p, sz );
                  size_start = &this->buffer[ size_start - p ];
                  p = this->buffer;
                }
                else { 
                  char *tmp = this->alloc( sz );
                  ::memcpy( tmp, p, sz );
                  size_start = &tmp[ size_start - p ];
                  p = tmp;
                }
                nargs = args.parse( &p[ 4 ], size_start );
                if ( nargs < 2 || nargs > 3 ) { /* optional reply */
                  fl |= DO_ERR;
                  break;
                }
                this->subject     = args.ptr[ 0 ]; 
                this->subject_len = args.len[ 0 ];
                this->sid         = args.ptr[ 1 ];
                this->sid_len     = args.len[ 1 ];
                if ( nargs > 2 ) {
                  this->reply     = args.ptr[ 2 ]; 
                  this->reply_len = args.len[ 2 ];
                }
                else {
                  this->reply     = NULL;
                  this->reply_len = 0;
                }
                this->msg_len_ptr    = &size_start[ 1 ];
                this->msg_len_digits = size_len;
                this->msg_state      = NATS_PUB_STATE;
                break;

              case NATS_KW_ERR:
                fl |= DO_ERR;
                fprintf( stderr, "%.*s", (int) sz, p );
                break;
              case NATS_KW_PING:
                this->append( pong, sizeof( pong ) - 1 );
                fl |= HAS_PING;
                break;
              case NATS_KW_INFO:
                this->parse_info( p, sz );
                break;
            }
          }
          p = &eol[ 1 ];
          used += sz;
        }
        else { /* no end of line */
          fl |= NEED_MORE;
        }
      }
      else { /* msg_state == NATS_PUB_STATE */
        if ( (size_t) ( end - p ) >= this->msg_len ) {
          this->msg_ptr = p;
          p = &p[ this->msg_len ];
          used += this->msg_len;
          this->msg_state = NATS_HDR_STATE;
          /* eat trailing crlf */
          while ( p < end && ( *p == '\r' || *p == '\n' ) ) {
            p++;
            used++;
          }
          if ( ! this->fwd_pub() )
            fl |= FLOW_BACKPRESSURE;
        }
        else { /* not enough to consume message */
          fl |= NEED_MORE;
        }
      }
      if ( ( fl & DO_ERR ) != 0 ) {
        this->push( EV_SHUTDOWN );
        break;
      }
      if ( ( fl & NEED_MORE ) != 0 ) {
        this->pushpop( EV_READ, EV_READ_LO );
        break;
      }
      if ( ( fl & ( FLOW_BACKPRESSURE | HAS_PING ) ) != 0 ) {
        this->off += used;
        if ( this->pending() > 0 )
          this->push( EV_WRITE_HI );
        if ( this->test( EV_READ ) )
          this->pushpop( EV_READ_LO, EV_READ );
        return;
      }
      fl = 0;
    }
    this->off += used;
    if ( used == 0 || ( fl & NEED_MORE ) != 0 )
      goto break_loop;
  }
break_loop:;
  this->pop( EV_PROCESS );
  this->push_write();
}

static inline void
decode_sub( char *p,  size_t sz )
{
  const char *start = p, *end = &p[ sz ];
  do {
    if ( *p == '<' || *p == '+' ) {
      if ( ( start == p       || p[ -1 ] == '.' ) &&
           ( end   == &p[ 1 ] || p[ 1 ] == '.' ) ) {
        if ( *p == '+' )
          *p = '*';
        else
          *p = '>';
      }
    }
    p++;
  } while ( --sz > 0 );
}

bool
EvNatsClient::fwd_pub( void ) noexcept
{
  decode_sub( this->subject, this->subject_len );
  NatsStr   xsub( this->subject, this->subject_len );
  EvPublish pub( this->subject, this->subject_len,
                 this->reply, this->reply_len,
                 this->msg_ptr, this->msg_len,
                 this->fd, xsub.hash(),
                 this->msg_len_ptr, this->msg_len_digits,
                 MD_STRING, 'p' );
  return this->poll.forward_msg( pub );
}

static inline char *
concat_hdr( char *p,  const char *q,  size_t sz )
{
  do { *p++ = *q++; } while ( --sz > 0 );
  return p;
}

static inline char *
encode_sub( char *p,  const char *q,  size_t sz )
{
  const char *start = q, *end = &q[ sz ];
  do {
    *p = *q;
    if ( *q == '>' || *q == '*' ) {
      if ( ( start == q       || q[ -1 ] == '.' ) &&
           ( end   == &q[ 1 ] || q[ 1 ] == '.' ) ) {
        if ( *q == '*' )
          *p = '+';
        else
          *p = '<';
      }
    }
    p++; q++;
  } while ( --sz > 0 );
  return p;
}

bool
EvNatsClient::on_msg( EvPublish &pub ) noexcept
{
  size_t msg_len_digits =
           ( pub.msg_len_digits > 0 ? pub.msg_len_digits :
             uint64_digits( pub.msg_len ) );
  size_t len = 4 +                                  /* PUB */
               pub.subject_len + 1 +                /* <subject> */
    ( pub.reply_len > 0 ? pub.reply_len + 1 : 0 ) + /* [reply] */
               msg_len_digits + 2 +             /* <size> \r\n */
               pub.msg_len + 2;                     /* <blob> \r\n */
  char *p = this->alloc( len );

  if ( pub.src_route == (uint32_t) this->fd )
    return true;
  p = concat_hdr( p, "PUB ", 4 );
  p = encode_sub( p, pub.subject, pub.subject_len );
  *p++ = ' ';
  if ( pub.reply_len > 0 ) {
    p = concat_hdr( p, (const char *) pub.reply, pub.reply_len );
    *p++ = ' ';
  }
  if ( pub.msg_len_digits == 0 ) {
    uint64_to_string( pub.msg_len, p, msg_len_digits );
    p = &p[ msg_len_digits ];
  }
  else {
    p = concat_hdr( p, pub.msg_len_buf, msg_len_digits );
  }

  *p++ = '\r'; *p++ = '\n';
  ::memcpy( p, pub.msg, pub.msg_len );
  p += pub.msg_len;
  *p++ = '\r'; *p++ = '\n';

  this->sz += len;
  bool flow_good = ( this->pending() <= this->send_highwater );
  this->idle_push( flow_good ? EV_WRITE : EV_WRITE_HI );
  return flow_good;
}

void
EvNatsClient::on_sub( uint32_t /*h*/,  const char *sub,  size_t sublen,
                      uint32_t /*fd*/,  uint32_t rcnt,  char /*tp*/,
                      const char * /*rep*/,  size_t /*rlen*/) noexcept
{
  if ( rcnt == 1 ) {
    size_t len = 4 +           /* SUB */
                 sublen + 1 +  /* <subject> */
                 sublen + 2;   /* <sid>\r\n */
    char *p = this->alloc( len )/*,
         *s = p*/;
    p = concat_hdr( p, "SUB ", 4 );
    p = concat_hdr( p, sub, sublen );
    *p++ = ' ';
    p = concat_hdr( p, sub, sublen );
    *p++ = '\r'; *p++ = '\n';
    /*printf( "%.*s", (int) len, s );*/
    this->sz += len;
    this->idle_push( EV_WRITE );
  }
}

void
EvNatsClient::on_unsub( uint32_t /*h*/,  const char *sub,  size_t sublen,
                        uint32_t /*fd*/,  uint32_t rcnt, char /*tp*/) noexcept
{
  if ( rcnt == 0 ) {
    size_t len = 6 +           /* UNSUB */
                 sublen + 2;   /* <sid>\r\n */
    char *p = this->alloc( len )/*,
         *s = p*/;
    p = concat_hdr( p, "UNSUB ", 6 );
    p = concat_hdr( p, sub, sublen );
    *p++ = '\r'; *p++ = '\n';
    /*printf( "%.*s", (int) len, s );*/
    this->sz += len;
    this->idle_push( EV_WRITE );
  }
}

void
EvNatsClient::on_psub( uint32_t /*h*/,  const char * /*pat*/,
                       size_t /*patlen*/,  const char *prefix,
                       uint8_t prefix_len,  uint32_t /*fd*/,  uint32_t rcnt,
                       char /*tp*/) noexcept
{
  if ( rcnt == 1 &&
       ( prefix_len == 0 || prefix[ prefix_len - 1 ] == '.' ) ) {
    size_t len = 4 +               /* SUB */
                 prefix_len + 2 +  /* <subject> + > */
                 prefix_len + 2;   /* <sid>\r\n */
    char *p = this->alloc( len )/*,
         *s = p*/;
    p = concat_hdr( p, "SUB ", 4 );
    p = concat_hdr( p, prefix, prefix_len );
    *p++ = '>'; *p++ = ' ';
    p = concat_hdr( p, prefix, prefix_len );
    *p++ = '\r'; *p++ = '\n';
    /*printf( "%.*s", (int) len, s );*/
    this->sz += len;
    this->idle_push( EV_WRITE );
  }
}

void
EvNatsClient::on_punsub( uint32_t /*h*/,  const char * /*pat*/,
                         size_t /*patlen*/,  const char *prefix,
                         uint8_t prefix_len,  uint32_t /*fd*/,
                         uint32_t rcnt,  char /*tp*/) noexcept
{
  if ( rcnt == 0 &&
       ( prefix_len == 0 || prefix[ prefix_len - 1 ] == '.' ) ) {
    size_t len = 6 +             /* UNSUB */
                 prefix_len + 2; /* <sid>\r\n */
    char *p = this->alloc( len )/*,
         *s = p*/;
    p = concat_hdr( p, "UNSUB ", 6 );
    p = concat_hdr( p, prefix, prefix_len );
    *p++ = '\r'; *p++ = '\n';
    /*printf( "%.*s", (int) len, s );*/
    this->sz += len;
    this->idle_push( EV_WRITE );
  }
}

void
EvNatsClient::parse_info( const char *info,  size_t infolen ) noexcept
{
  char buf[ 1024 ], *p = buf;
  size_t bsz = sizeof( buf );
  int len;

  printf( "%.*s", (int) infolen, info );
  len = snprintf( p, bsz, "CONNECT {" );
  p = &buf[ len ]; bsz = sizeof( buf ) - len;
  if ( this->name != NULL ) {
    len += snprintf( p, bsz, "\"name\":\"%s\",", this->name );
    p = &buf[ len ]; bsz = sizeof( buf ) - len;
  }
  if ( this->user != NULL ) {
    len += snprintf( p, bsz, "\"user\":\"%s\",", this->user );
    p = &buf[ len ]; bsz = sizeof( buf ) - len;
  }
  if ( this->pass != NULL ) {
    len += snprintf( p, bsz, "\"pass\":\"%s\",", this->pass );
    p = &buf[ len ]; bsz = sizeof( buf ) - len;
  }
  if ( this->auth_token != NULL ) {
    len += snprintf( p, bsz, "\"auth_token\":\"%s\",", this->auth_token );
    p = &buf[ len ]; bsz = sizeof( buf ) - len;
  }
  if ( this->lang != NULL ) {
    len += snprintf( p, bsz, "\"lang\":\"%s\",", this->lang );
    p = &buf[ len ]; bsz = sizeof( buf ) - len;
  }
  if ( this->version != NULL ) {
    len += snprintf( p, bsz, "\"version\":\"%s\",", this->version );
    p = &buf[ len ]; bsz = sizeof( buf ) - len;
  }
  len += snprintf( &buf[ len ], bsz, "\"verbose\":false,\"echo\":false}\r\n" );

  printf( "%.*s", len, buf );
  this->append( buf, len );
  this->poll.add_route_notify( *this );
  uint32_t h = this->poll.sub_route.prefix_seed( 0 );
  this->poll.sub_route.add_pattern_route( h, this->fd, 0 );
  this->on_connect();
}
/* notifications */
void EvNatsClient::on_connect( void ) noexcept {}
void EvNatsClient::on_shutdown( uint64_t ) noexcept {}

void
EvNatsClient::release( void ) noexcept
{
  uint64_t bytes_lost = this->pending();
  uint32_t h = this->poll.sub_route.prefix_seed( 0 );
  this->poll.sub_route.del_pattern_route( h, this->fd, 0 );
  this->poll.remove_route_notify( *this );
  this->EvConnection::release_buffers();
  this->on_shutdown( bytes_lost );
}

