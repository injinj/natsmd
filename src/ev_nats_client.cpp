#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <unistd.h>
#else
#include <raikv/win.h>
#endif
#include <natsmd/ev_nats_client.h>
#include <raikv/ev_publish.h>
#include <raikv/bit_set.h>
#include <raikv/pattern_cvt.h>
#include <raimd/json_msg.h>

using namespace rai;
using namespace natsmd;
using namespace kv;
using namespace md;

int nats_client_pub_verbose,
    nats_client_msg_verbose,
    nats_client_sub_verbose,
    nats_client_info_verbose,
    nats_client_cmd_verbose,
    nats_client_init;

static int
getenv_bool( const char *var )
{
  const char *val = ::getenv( var );
  return val != NULL && val[ 0 ] != 'f' && val[ 0 ] != '0';
}

EvNatsClient::EvNatsClient( EvPoll &p ) noexcept
    : EvConnection( p, p.register_type( "natsclient" ) ),
      RouteNotify( p.sub_route ),
      sub_route( p.sub_route ), cb( 0 ),
      next_sid( 1 ), protocol( 1 ), fwd_all_msgs( 1 ), fwd_all_subs( 1 ),
      max_payload( 1024 * 1024 ), name( 0 ),
      lang( 0 ), version( 0 ), user( 0 ), pass( 0 ), auth_token( 0 ),
      sid_ht( 0 )
{
  if ( ! nats_client_init ) {
    nats_client_init = 1;
    nats_client_pub_verbose   = getenv_bool( "NATS_CLIENT_PUB_VERBOSE" );
    nats_client_msg_verbose   = getenv_bool( "NATS_CLIENT_MSG_VERBOSE" );
    nats_client_sub_verbose   = getenv_bool( "NATS_CLIENT_SUB_VERBOSE" );
    nats_client_info_verbose  = getenv_bool( "NATS_CLIENT_INFO_VERBOSE" );
    nats_client_cmd_verbose   = getenv_bool( "NATS_CLIENT_CMD_VERBOSE" );
    int all_verbose           = getenv_bool( "NATS_CLIENT_VERBOSE" );
    nats_client_pub_verbose  |= all_verbose;
    nats_client_msg_verbose  |= all_verbose;
    nats_client_sub_verbose  |= all_verbose;
    nats_client_info_verbose |= all_verbose;
    nats_client_cmd_verbose  |= all_verbose;
  }
}

bool NatsClientCB::on_msg( EvPublish & ) noexcept { return true; }

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
EvNatsClient::connect( EvNatsClientParameters &p,
                       EvConnectionNotify *n,
                       NatsClientCB *c ) noexcept
{
  char * daemon = NULL, buf[ 256 ];
  int port = p.port;
  if ( this->fd != -1 )
    return false;

  if ( p.host != NULL ) {
    size_t len = ::strlen( p.host );
    if ( len >= sizeof( buf ) )
      len = sizeof( buf ) - 1;
    ::memcpy( buf, p.host, len );
    buf[ len ] = '\0';
    daemon = buf;
  }
  if ( daemon != NULL ) {
    char * pt;
    if ( (pt = ::strrchr( daemon, ':' )) != NULL ) {
      port = atoi( pt + 1 );
      *pt = '\0';
    }
    else {
      for ( pt = daemon; *pt != '\0'; pt++ )
        if ( *pt < '0' || *pt > '9' )
          break;
      if ( *pt == '\0' ) {
        port = atoi( daemon );
        daemon = NULL;
      }
    }
    if ( daemon != NULL ) { /* strip tcp: prefix */
      if ( ::strncmp( daemon, "tcp:", 4 ) == 0 )
        daemon += 4;
      if ( ::strcmp( daemon, "tcp" ) == 0 )
        daemon += 3;
      if ( daemon[ 0 ] == '\0' )
        daemon = NULL;
    }
  }

  this->initialize_state();
  if ( EvTcpConnection::connect( *this, daemon, port,
                                 DEFAULT_TCP_CONNECT_OPTS ) != 0 )
    return false;
  this->name       = p.name;
  this->lang       = p.lang;
  this->version    = p.version;
  this->user       = p.user;
  this->pass       = p.pass;
  this->auth_token = p.auth_token;
  this->notify     = n;
  this->cb         = c;
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
  static const char pong[] = "PONG\r\n";

  for (;;) {
    if ( this->off == this->len )
      break;

    NatsMsg msg;
    int fl = msg.parse_msg( &this->recv[ this->off ], &this->recv[ this->len ]);
    if ( fl == NEED_MORE ) {
      if ( msg.size > 0 )
        this->recv_need( msg.size );
      break;
    }
    switch ( fl ) {
      case RCV_MSG:
      case HRCV_MSG:
        if ( ! this->fwd_pub( msg ) )
          fl |= FLOW_BACKPRESSURE;
        break;

      case IS_ERR:
        fprintf( stderr, "%.*s", (int) msg.size, msg.line );
        /* if error occurs, shutdown read side, flush writes */
        this->push( EV_SHUTDOWN );
        goto break_loop;

      case IS_PING:
        this->append( pong, sizeof( pong ) - 1 );
        break;

      case IS_INFO:
        this->parse_info( msg.line, msg.size );
        break;

      default:
        break;
    }
    this->off += msg.size;

    /* write buffer immediately when ping or publish back pressure */
    if ( ( fl & FLOW_BACKPRESSURE ) != 0 || fl == IS_PING ) {
      if ( this->pending() > 0 )
        this->push( EV_WRITE_HI );
      if ( this->test( EV_READ ) )
        this->pushpop( EV_READ_LO, EV_READ );
      return;
    }
  }
break_loop:;
  this->pop( EV_PROCESS );
  if ( ! this->push_write() )
    this->clear_write_buffers();
}

void
EvNatsClient::make_session( void ) noexcept
{
  uint64_t now = kv_current_realtime_ns(), mo = kv_current_monotonic_time_ns();
  char host[ 256 ], inbox[ 8 + 64 ];
  ::gethostname( host, sizeof( host ) );
  uint32_t x[ 2 ];
  x[ 0 ] = kv_crc_c( host, ::strlen( host ), (uint32_t) now );
  x[ 1 ] = kv_hash_uint2( x[ 0 ], (uint32_t) mo );
  this->session_len =
    bin_to_base64( x, sizeof( x ), this->session, false );
  this->session[ this->session_len ] = '\0';
  for ( size_t i = 0; i < this->session_len; i++ ) {
    if ( this->session[ i ] == '+' || this->session[ i ] == '/' ) {
      this->session[ i ] = 'a' + ( now % 26 );
      now /= 26;
    }
  }
  ::memcpy( inbox, "_INBOX.", 7 );
  ::memcpy( &inbox[ 7 ], this->session, this->session_len );

  size_t inbox_len = 7 + this->session_len;
  inbox[ inbox_len++ ] = '.';
  uint32_t h = kv_crc_c( inbox, inbox_len,
                         this->sub_route.prefix_seed( inbox_len ) );
  this->do_psub( h, inbox, inbox_len, NULL, 0 );
  this->idle_push_write();
}

uint16_t
EvNatsClient::make_inbox( char *inbox,  uint64_t num ) noexcept
{
  int16_t off = 7;
  ::memcpy( inbox, "_INBOX.", off );
  if ( this->session_len == 0 )
    this->make_session();
  ::memcpy( &inbox[ off ], this->session, this->session_len );
  off += this->session_len;
  inbox[ off++ ] = '.';
  off += uint64_to_string( num, &inbox[ off ] );
  inbox[ off ] = '\0';
  return off;
}

uint64_t
EvNatsClient::is_inbox( const char *sub,  size_t sublen ) noexcept
{
  if ( sublen > 7 + (size_t) this->session_len + 1 ) {
    if ( ::memcmp( sub, "_INBOX.", 7 ) == 0 &&
         ::memcmp( &sub[ 7 ], this->session, this->session_len ) == 0 &&
         sub[ 7 + this->session_len ] == '.' ) {
      size_t off = 7 + this->session_len + 1;
      if ( sub[ off ] >= '0' && sub[ off ] <= '9' )
        return string_to_uint64( &sub[ off ], sublen - off );
    }
  }
  return 0;
}

void
EvNatsClient::save_error( const char *,  size_t ) noexcept
{
#if 0
  /* trim whitespace */
  while ( len > 0 && buf[ len - 1 ] <= ' ' )
    len--;
  /* skip -ERR if there is a description */
  if ( len > 5 && ::memcmp( buf, "-ERR ", 5 ) == 0 ) {
    buf = &buf[ 5 ];
    len -= 5;
  }
  /* strip quotes */
  if ( len > 2 && buf[ 0 ] == '\'' && buf[ len - 1 ] == '\'' ) {
    buf++;
    len -= 2;
  }
  if ( this->err_len > 0 )
    this->tmp_size += this->err_len; /* free old error */
  this->err_len = len;
  if ( this->err_len > this->tmp_size )
    this->err_len = this->tmp_size;
  this->tmp_size -= this->err_len;

  if ( this->err_len > 0 ) {
    this->err = &this->buffer[ this->tmp_size ];
    ::memcpy( this->err, buf, this->err_len );
  }
  else {
    this->err = NULL;
  }
#endif
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
EvNatsClient::fwd_pub( NatsMsg &msg ) noexcept
{
  decode_sub( msg.subject, msg.subject_len );
  NatsStr   xsub( msg.subject, msg.subject_len );
  EvPublish pub( msg.subject, msg.subject_len,
                 msg.reply, msg.reply_len,
                 msg.msg_ptr, msg.msg_len,
                 this->sub_route, *this, xsub.hash(),
                 MD_STRING, 'p' );
  uint32_t       pmatch;
  bool           flow = true;
  NatsFragment * frag = NULL;

  pub.hdr_len = msg.hdr_len;
  /* if msg is at max_payload or a trailer fragment */
  if ( msg.msg_len == this->max_payload ||
       ! this->frags_pending.is_empty() ) {
    NatsTrailer trail( msg.msg_ptr, msg.msg_len );
    if ( trail.is_fragment( xsub.hash(), this->max_payload ) ) {
      if ( (frag = this->merge_fragment( trail, msg.msg_ptr,
                                         msg.msg_len )) != NULL ) {
        pub.msg_len = frag->msg_len;
        pub.msg     = frag->msg_ptr();
      }
      else {
        return true; /* partial message, wait for all frags */
      }
    }
  }
  if ( nats_client_pub_verbose )
    printf( "fwd_pub(%.*s) reply(%.*s)\n",
            (int) pub.subject_len, pub.subject,
            (int) pub.reply_len, (char *) pub.reply );
  /* negative sids are wildcard matches */
  if ( msg.sid[ 0 ] == '-' ) /* wild matches, check if another wild match */
    pmatch = this->possible_matches( msg.subject[ 0 ] );
  else if ( this->test_wildcard_match( msg.subject[ 0 ] ) )
    pmatch = 2; /* normal subject and wildcard */
  else
    pmatch = 1; /* only the subject matches */
  /* if only one possible match, no need to deduplicate */
  if ( pmatch <= 1 ) {
    if ( this->cb != NULL )
      flow = this->cb->on_msg( pub );
    else
      flow = this->sub_route.forward_msg( pub );
  }
  else { /* multiple wildcards or a subject and a wildcard may match */
    flow = this->deduplicate_wildcard( msg, pub );
  }
  if ( frag != NULL )
    delete frag;
  return flow;
}

/* multiple messages may be published for matching wildcards,
 * for example, TEST.SUBJECT matches > and TEST.>, each with
 * their own sid */
bool
EvNatsClient::deduplicate_wildcard( NatsMsg &msg,  EvPublish &pub ) noexcept
{
  BitSet64 bi( this->sub_route.pat_mask );
  uint32_t i, hash,
           max_sid = 0;
  /* bitmask of prefix lengths, 0 -> 63 chars */
  if ( bi.first( i ) ) {
    /* if didn't hash prefixes */
    do {
      if ( i > msg.subject_len )
        break;
      hash = this->sub_route.prefix_seed( i );
      if ( i > 0 )
        hash = kv_crc_c( msg.subject, i, hash );
      NatsPrefix *pref = this->pat_tab.find( hash, msg.subject,
                                             (size_t) i );
      if ( pref != NULL ) {
        if ( msg.sid[ 0 ] != '-' ) /* not a wildcard sid */
          return true; /* matches a wildcard, toss the subject publish */
        if ( pref->sid > max_sid ) /* find the maximum sid */
          max_sid = pref->sid;
      }
      /* track the prefix hashes, it is used by poll.forward_msg() */
    } while ( bi.next( i ) );
  }
  /* if no wildcard matches or the maximum sid matches, forward */
  if ( max_sid == 0 || (uint64_t) max_sid ==
                       string_to_uint64( &msg.sid[ 1 ], msg.sid_len - 1 ) ) {
    if ( this->cb != NULL )
      return this->cb->on_msg( pub );
    return this->sub_route.forward_msg( pub );
  }
  /* toss the publish, only forward the maximum sid */
  return true;
}
/* if message is a fragment, find and merge into other fragments */
NatsFragment *
EvNatsClient::merge_fragment( NatsTrailer &trail,  const void *msg,
                              size_t msg_len ) noexcept
{
  NatsFragment *p;
  /* all frags pending are in a list */
  for ( p = this->frags_pending.hd; p != NULL; p = p->next ) {
    if ( p->src_id   == trail.src_id &&   /* the source matches */
         p->src_time == trail.src_time && /* the time the message created */
         p->hash     == trail.hash &&     /* the subject hash */
         p->msg_len  == trail.msg_len )   /* the total length of the message */
      break;
  }
  /* if no frag matches, must be the first fragment */
  if ( p == NULL ) {
    /* must start at 0 */
    if ( trail.off != 0 ) {
      fprintf( stderr, "fragment ignored, not starting at the head\n" );
      return NULL;
    }
    /* must be larger than max_payload */
    if ( trail.msg_len < this->max_payload ) {
      fprintf( stderr, "fragment ignored, msg_len %u is less than payload\n",
               trail.msg_len );
      return NULL;
    }
    /* allocate space for entire message */
    void *m = ::malloc( trail.msg_len + sizeof( NatsFragment ) );
    if ( m == NULL ) {
      fprintf( stderr, "can't allocated fragment size %u\n", trail.msg_len );
    }
    /* push onto frag list */
    p = new ( m ) NatsFragment( trail.src_id, trail.src_time, trail.hash,
                                trail.msg_len );
    this->frags_pending.push_hd( p );
  }
  /* frags are published in offset order */
  if ( trail.off != p->off ) {
    fprintf( stderr, "fragment offset %u:%u missing data\n", trail.off, p->off );
    this->frags_pending.pop( p );
    delete p;
    return NULL;
  }
  /* merge fragment */
  uint8_t * frag_msg  = (uint8_t *) p->msg_ptr();
  size_t    frag_size = msg_len - sizeof( NatsTrailer );
  ::memcpy( &frag_msg[ trail.off ], msg, frag_size );
  p->off += (uint32_t) frag_size;
  /* if all fragments are recvd */
  if ( p->off == p->msg_len ) {
    this->frags_pending.pop( p );
    return p;
  }
  return NULL;
}
void
EvNatsClient::process_close( void ) noexcept
{
  this->client_stats( this->sub_route.peer_stats );
  this->EvSocket::process_close();
}

/* clear fragmens on shutdown */
void
EvNatsClient::release_fragments( void ) noexcept
{
  if ( ! this->frags_pending.is_empty() ) {
    NatsFragment * next;
    for ( NatsFragment *p = this->frags_pending.hd; p != NULL; p = next ) {
      next = p->next;
      delete p;
    }
    this->frags_pending.init();
  }
}
/* used to create a message */
static inline char *
concat_hdr( char *p,  const char *q,  size_t sz )
{
  do { *p++ = *q++; } while ( --sz > 0 );
  return p;
}
/* published subject wildcards X.*.Z and X.> are encoded as X.+.Z and X.< */
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
/* forward a publish to the NATS network */
bool
EvNatsClient::on_msg( EvPublish &pub ) noexcept
{
  if ( this->equals( pub.src_route ) ) /* no echo */
    return true;
  return this->publish( pub );
}

bool
EvNatsClient::publish( EvPublish &pub ) noexcept
{
  char * p;
  size_t len, msg_len_digits;

  if ( nats_client_msg_verbose )
    printf( "on_msg(%.*s) reply(%.*s)\n",
            (int) pub.subject_len, pub.subject,
            (int) pub.reply_len, (char *) pub.reply );
  /* construct PUB subject <reply> <length \r\n <blob> */
  if ( pub.msg_len <= this->max_payload ) {
    msg_len_digits = uint64_digits( pub.msg_len );
    len = 4 + pub.subject_len + 1 +                /* PUB <subject> */
      ( pub.reply_len > 0 ? pub.reply_len + 1 : 0 ) + /* [reply] */
                 msg_len_digits + 2 +             /* <size> \r\n */
                 pub.msg_len + 2;                     /* <blob> \r\n */
    p = concat_hdr( this->alloc( len ), "PUB ", 4 );
    p = encode_sub( p, pub.subject, pub.subject_len );
    *p++ = ' ';
    if ( pub.reply_len > 0 ) {
      p = concat_hdr( p, (const char *) pub.reply, pub.reply_len );
      *p++ = ' ';
    }
    uint64_to_string( pub.msg_len, p, msg_len_digits );
    p = &p[ msg_len_digits ];

    *p++ = '\r'; *p++ = '\n';
    ::memcpy( p, pub.msg, pub.msg_len );
    p += pub.msg_len;
    *p++ = '\r'; *p++ = '\n';

    this->sz += len;
  }
  /* fragment PUB into multiple messages, each with the same subject */
  else {
    NatsTrailer trail( this->poll.create_ns(),
                       this->poll.current_coarse_ns(),
                       pub.subj_hash,
                       pub.msg_len );
    size_t      frag_size = this->max_payload - sizeof( NatsTrailer ),
                msg_len   = this->max_payload;
    bool        is_last   = false;

    msg_len_digits = uint64_digits( msg_len );
    for ( trail.off = 0; trail.off < pub.msg_len;
          trail.off += (uint32_t) frag_size ) {
      if ( trail.off + frag_size > pub.msg_len ) {
        frag_size      = pub.msg_len - trail.off;
        msg_len        = frag_size + sizeof( NatsTrailer );
        msg_len_digits = uint64_digits( msg_len );
      }
      if ( trail.off + frag_size == pub.msg_len ) {
        len = ( pub.reply_len > 0 ? pub.reply_len + 1 : 0 ); /* [reply] */
        is_last = true;
      }
      else {
        len = 0;
      }
      len += 4 + pub.subject_len + 1 + /* PUB <subject> */
             msg_len_digits + 2 +      /* <size> \r\n */
             msg_len + 2;              /* <blob> \r\n */

      p = concat_hdr( this->alloc( len ), "PUB ", 4 );
      p = encode_sub( p, pub.subject, pub.subject_len );
      *p++ = ' ';
      if ( is_last && pub.reply_len > 0 ) {
        p = concat_hdr( p, (const char *) pub.reply, pub.reply_len );
        *p++ = ' ';
      }
      uint64_to_string( msg_len, p, msg_len_digits );
      p = &p[ msg_len_digits ];
      *p++ = '\r'; *p++ = '\n';
      ::memcpy( p, &((uint8_t *) pub.msg)[ trail.off ], frag_size );
      p += frag_size;
      ::memcpy( p, &trail, sizeof( NatsTrailer ) );
      p += sizeof( NatsTrailer );
      *p++ = '\r'; *p++ = '\n';

      this->sz += len;
    }
  }
  return this->idle_push_write();
}

/* hash subject to insert sid into sid_ht,
 * if collision, use hash64 in sid_collsion_ht */
uint32_t
EvNatsClient::create_sid( uint32_t h, const char *sub, size_t sublen,
                          bool &is_new ) noexcept
{
  SidHash  hash( h, sub, sublen );
  size_t   pos;
  uint32_t sid;

  if ( ! this->sid_ht->find( hash, pos, sid ) ) {
    sid = this->next_sid++;
    is_new = true;
    this->sid_ht->set( hash, pos, sid );
    if ( this->sid_ht->need_resize() )
      this->sid_ht = SidHashTab::resize( this->sid_ht );
  }
  else {
    is_new = false;
  }
  return sid;
}
/* find sid of subject and remove it */
uint32_t
EvNatsClient::remove_sid( uint32_t h, const char *sub, size_t sublen ) noexcept
{
  SidHash  hash( h, sub, sublen );
  size_t   pos;
  uint32_t sid;
  /* it must be in sid_ht */
  if ( this->sid_ht == NULL || ! this->sid_ht->find( hash, pos, sid ) ) {
    fprintf( stderr, "sub %.*s not subscribed\n", (int) sublen, sub );
    return 0; /* not a valid sid */
  }
  this->sid_ht->remove( pos );
  if ( this->sid_ht->need_resize() )
    this->sid_ht = SidHashTab::resize( this->sid_ht );
  return sid;
}
/* forward subscribe subject: SUB subject sid */
void
EvNatsClient::do_sub( uint32_t h,  const char *sub,  size_t sublen,
                      const char *queue,  size_t queue_len ) noexcept
{
  bool     is_new;
  uint32_t sid        = this->create_sid( h, sub, sublen, is_new ),
           sid_digits = (uint32_t) uint32_digits( sid );
  size_t   len = 4 +             /* SUB */
                 sublen + 1 +    /* <subject> */
                 queue_len + 1 + /* <queue>   */
                 sid_digits + 2; /* <sid>\r\n */
  char *p = this->alloc( len );
  CatPtr hdr( p );
  hdr.s( "SUB " ).x( sub, sublen ).s( " " );
  if ( queue_len > 0 )
    hdr.x( queue, queue_len ).s( " " );
  hdr.u( sid, sid_digits ).s( "\r\n" );
  this->sz += hdr.len();
  if ( nats_client_sub_verbose )
    printf( "%.*s", (int) hdr.len(), p );
}

const char *
EvNatsClient::is_wildcard( const char *subject,  size_t subject_len ) noexcept
{
  const char * p   = (const char *) ::memchr( subject, '*', subject_len ),
             * end = &subject[ subject_len ];
  while ( p != NULL ) {
    if ( ( p == subject || *(p-1) == '.' ) &&
         ( p+1 == end || *(p+1) == '.' ) )
      return p;
    if ( p+1 == end )
      break;
    p = (const char *) ::memchr( p+1, '*', end - (p+1) );
  }
  if ( subject_len >= 1 && subject[ subject_len - 1 ] == '>' &&
       ( subject_len == 1 || subject[ subject_len - 2 ] == '.' ) ) {
    return &subject[ subject_len - 1 ];
  }
  return NULL;
}
void
EvNatsClient::subscribe( const char *subject,  size_t subject_len,
                         const char *queue,  size_t queue_len ) noexcept
{
  const char *pre;
  uint32_t h;
  if ( (pre = this->is_wildcard( subject, subject_len )) != NULL ) {
    size_t prefix_len = pre - subject;
    if ( prefix_len >= MAX_PRE )
      prefix_len = MAX_PRE - 1;
    h = kv_crc_c( subject, prefix_len,
                  this->sub_route.prefix_seed( prefix_len ) );
    this->do_psub( h, subject, prefix_len, queue, queue_len );
  }
  else {
    h = kv_crc_c( subject, subject_len, 0 );
    this->do_sub( h, subject, subject_len, queue, queue_len );
  }
  this->idle_push_write();
}
/* forward subscribe subject: SUB subject sid */
void
EvNatsClient::on_sub( NotifySub &sub ) noexcept
{
  this->do_sub( sub.subj_hash, sub.subject, sub.subject_len, NULL, 0 );
  this->idle_push_write();
}
void
EvNatsClient::do_unsub( uint32_t h,  const char *sub,  size_t sublen ) noexcept
{
  uint32_t sid        = this->remove_sid( h, sub, sublen ),
           sid_digits = (uint32_t) uint32_digits( sid );
  if ( sid != 0 ) {
    size_t   len = 6 +             /* UNSUB */
                   sid_digits + 2; /* <sid>\r\n */
    char *p = this->alloc( len ),
         *s = p;
    p = concat_hdr( p, "UNSUB ", 6 );
    uint32_to_string( sid, p, sid_digits );
    p = &p[ sid_digits ];
    *p++ = '\r'; *p++ = '\n';
    if ( nats_client_sub_verbose )
      printf( "%.*s", (int) len, s );
    this->sz += len;
    this->idle_push_write();
  }
}
/* forward unsubscribe subject: UNSUB sid */
void
EvNatsClient::on_unsub( NotifySub &sub ) noexcept
{
  if ( sub.sub_count != 0 ) /* if no routes left */
    return;
  this->do_unsub( sub.subj_hash, sub.subject, sub.subject_len );
}
void
EvNatsClient::unsubscribe( const char *subject,  size_t subject_len ) noexcept
{
  const char *pre;
  uint32_t h;
  if ( (pre = this->is_wildcard( subject, subject_len )) != NULL ) {
    size_t prefix_len = pre - subject;
    if ( prefix_len >= MAX_PRE )
      prefix_len = MAX_PRE - 1;
    h = kv_crc_c( subject, prefix_len,
                  this->sub_route.prefix_seed( prefix_len ) );
    this->do_punsub( h, subject, prefix_len );
  }
  else {
    h = kv_crc_c( subject, subject_len, 0 );
    this->do_unsub( h, subject, subject_len );
  }
}
/* forward pattern subscribe: SUB wildcard -sid */
void
EvNatsClient::do_psub( uint32_t h,  const char *prefix,
                       size_t prefix_len,  const char *queue,
                       size_t queue_len ) noexcept
{
  uint8_t w = ( prefix_len == 0 ? 0 : prefix[ 0 ] );
  bool         is_new;
  uint32_t     sid        = this->create_sid( h, prefix, prefix_len, is_new ),
               sid_digits = (uint32_t) uint32_digits( sid );
  size_t       len        = 4 +                 /* SUB */
                            prefix_len + 2 +    /* <subject> + > */
                            queue_len + 1 +     /* <queue> */
                            1 + sid_digits + 2; /* -<sid>\r\n */
  NatsPrefix * pat;
  if ( is_new ) {
    this->set_wildcard_match( w ); /* track first char of wildcards */
    pat = this->pat_tab.insert( h, prefix, prefix_len );
    if ( pat == NULL ) {
      fprintf( stderr, "pattern insert error: %.*s\n", (int) prefix_len, prefix );
      return;
    }
    pat->sid = sid;
  }
  else {
    pat = this->pat_tab.find( h, prefix, (size_t) prefix_len );
    if ( pat == NULL ) { /* should be there */
      fprintf( stderr, "pattern not found: %.*s\n", (int) prefix_len, prefix );
      return;
    }
    if ( pat->sid != sid ) {
      fprintf( stderr, "bad sid for prefix: %.*s\n", (int) prefix_len, prefix );
      pat->sid = sid;
    }
  }
  char *p = this->alloc( len );
  CatPtr hdr( p );
  hdr.s( "SUB " ).x( prefix, prefix_len ).s( "> " );
  if ( queue_len > 0 )
    hdr.x( queue, queue_len ).s( " " );
  hdr.s( "-" ).u( sid, sid_digits ).s( "\r\n" );
  this->sz += hdr.len();
  if ( nats_client_sub_verbose )
    printf( "%.*s", (int) hdr.len(), p );
}
/* forward pattern subscribe: SUB wildcard -sid */
void
EvNatsClient::on_psub( NotifyPattern &pat ) noexcept
{
  size_t prefix_len = pat.cvt.prefixlen;
  const char * prefix = pat.pattern;
  bool fwd;
  /* prefix must end with a '.' */
  if ( prefix_len > 0 && prefix[ prefix_len - 1 ] == '.' )
    fwd = true;
  else if ( prefix_len > 0 ) {
    fprintf( stderr, "unable psub, no segment \"%.*s\"\n", 
             (int) prefix_len, prefix );
    fwd = false;
  }
  else
    fwd = true;
  if ( ! fwd )
    return;
  this->do_psub( pat.prefix_hash, prefix, prefix_len, NULL, 0 );
  this->idle_push_write();
}
/* forward pattern unsubscribe: UNSUB -sid */
void
EvNatsClient::on_punsub( NotifyPattern &pat ) noexcept
{
  size_t prefix_len = pat.cvt.prefixlen;
  const char * prefix = pat.pattern;
  if ( pat.sub_count != 0 ) /* if no routes left */
    return;
  bool fwd;
  /* prefix must end with a '.' */
  if ( prefix_len > 0 && prefix[ prefix_len - 1 ] == '.' )
    fwd = true;
  else if ( prefix_len > 0 ) {
    fprintf( stderr, "unable punsub, no segment \"%.*s\"\n", 
             (int) prefix_len, prefix );
    fwd = false;
  }
  else
    fwd = true;
  if ( ! fwd )
    return;
  this->do_punsub( pat.prefix_hash, prefix, prefix_len );
}

void
EvNatsClient::do_punsub( uint32_t h,  const char *prefix,
                         size_t prefix_len ) noexcept
{
  uint32_t sid        = this->remove_sid( h, prefix, prefix_len ),
           sid_digits = (uint32_t) uint32_digits( sid );
  if ( sid != 0 ) {
    uint8_t w = ( prefix_len == 0 ? 0 : prefix[ 0 ] );
    this->clear_wildcard_match( w );
    this->pat_tab.remove( h, prefix, (size_t) prefix_len );

    size_t   len        = 6 +                 /* UNSUB */
                          1 + sid_digits + 2; /* -<sid>\r\n */
    char *p = this->alloc( len ),
         *s = p;
    p = concat_hdr( p, "UNSUB ", 6 );
    *p++ = '-';
    uint32_to_string( sid, p, sid_digits );
    p = &p[ sid_digits ];
    *p++ = '\r'; *p++ = '\n';
    this->sz += len;
    if ( nats_client_sub_verbose )
      printf( "%.*s", (int) len, s );
    this->idle_push_write();
  }
}

void
EvNatsClient::on_reassert( uint32_t /*fd*/,  RouteVec<RouteSub> &sub_db,
                           RouteVec<RouteSub> &pat_db ) noexcept
{
  RouteLoc   loc;
  RouteSub * sub;

  for ( sub = sub_db.first( loc ); sub != NULL; sub = sub_db.next( loc ) ) {
    this->do_sub( sub->hash, sub->value, sub->len, NULL, 0 );
  }
  for ( sub = pat_db.first( loc ); sub != NULL; sub = pat_db.next( loc ) ) {
    this->do_psub( sub->hash, sub->value, sub->len, NULL, 0 );
  }
  this->idle_push_write();
}

/* parse json formatted message:
 * INFO { server_id:"id",server_name:"svr",go:"1.0",max_payload:size ... }
 * then forward a connect message:
 * CONNECT { name:"nm",user:"u",pass:"p" ... } */
void
EvNatsClient::parse_info( const char *buf,  size_t bufsz ) noexcept
{
  const char * start, * end;/*, * p, * v, * comma;*/

  if ( nats_client_info_verbose )
    printf( "%.*s", (int) bufsz, buf );
  if ( (start = (const char *) ::memchr( buf, '{', bufsz )) != NULL ) {
    bufsz -= start - buf;
    for ( end = &start[ bufsz ]; end > start; )
      if ( *--end == '}' )
        break;

    if ( end > start ) {
      MDMsgMem      mem;
      JsonMsg     * msg;
      MDFieldIter * iter;
      MDName        name;
      MDReference   mref;
      msg = JsonMsg::unpack( (void *) start, 0, &end[ 1 ] - start, 0, NULL,
                             &mem );
      if ( msg != NULL ) {
        if ( msg->get_field_iter( iter ) == 0 ) {
          if ( iter->first() == 0 ) {
            do {
              if ( iter->get_name( name ) == 0 ) {
                if ( name.fnamelen <= 4 ) /* "go:"str" */
                  continue;
                /* need max payload */
                switch ( ( unaligned<uint32_t>( name.fname ) ) & 0xdfdfdfdf ) {
                  case NATS_JS_MAX_PAYLOAD: /* max_payload:1048576 */
                    if ( iter->get_reference( mref ) == 0 )
                      cvt_number( mref, this->max_payload );
                    break;
                  case NATS_JS_PROTOCOL: /* proto:1 */
                    if ( iter->get_reference( mref ) == 0 )
                      cvt_number( mref, this->protocol );
                    break;
                  case NATS_JS_SERVER:  /* server_id:"str", server_name:"str" */
                  case NATS_JS_CONNECT_URLS: /* connect_urls:["url1","url2"] */
                  case NATS_JS_CLIENT:  /* client_id:12, client_ip:"x.x.x.x" */
                  case NATS_JS_HOST:       /* host:"x.x.x.x" */
                  case NATS_JS_PORT:       /* port:4222 */
                  case NATS_JS_GIT_COMMIT: /* git_commit:"str" */
                  case NATS_JS_VERSION:    /* version:"str" */
                  default:
                    break;
                }
              }
            } while ( iter->next() == 0 );
          }
        }
      }
    }
  }
  char outbuf[ 1024 ], *o = outbuf;
  size_t bsz = sizeof( outbuf );
  int n, len = snprintf( o, bsz, "CONNECT {" );
  o = &outbuf[ len ]; bsz = sizeof( outbuf ) - len;
  if ( this->name != NULL ) {
    n = snprintf( o, bsz, "\"name\":\"%s\",", this->name );
    len += min_int( n, (int) bsz - 1 );
    o = &outbuf[ len ]; bsz = sizeof( outbuf ) - len;
  }
  if ( this->user != NULL && (size_t) len < bsz ) {
    n = snprintf( o, bsz, "\"user\":\"%s\",", this->user );
    len += min_int( n, (int) bsz - 1 );
    o = &outbuf[ len ]; bsz = sizeof( outbuf ) - len;
  }
  if ( this->pass != NULL && (size_t) len < bsz ) {
    n = snprintf( o, bsz, "\"pass\":\"%s\",", this->pass );
    len += min_int( n, (int) bsz - 1 );
    o = &outbuf[ len ]; bsz = sizeof( outbuf ) - len;
  }
  if ( this->auth_token != NULL && (size_t) len < bsz ) {
    n = snprintf( o, bsz, "\"auth_token\":\"%s\",", this->auth_token );
    len += min_int( n, (int) bsz - 1 );
    o = &outbuf[ len ]; bsz = sizeof( outbuf ) - len;
  }
  if ( this->lang != NULL && (size_t) len < bsz ) {
    n = snprintf( o, bsz, "\"lang\":\"%s\",", this->lang );
    len += min_int( n, (int) bsz - 1 );
    o = &outbuf[ len ]; bsz = sizeof( outbuf ) - len;
  }
  if ( this->version != NULL && (size_t) len < bsz ) {
    n = snprintf( o, bsz, "\"version\":\"%s\",", this->version );
    len += min_int( n, (int) bsz - 1 );
    o = &outbuf[ len ]; bsz = sizeof( outbuf ) - len;
  }
  if ( (size_t) len < bsz ) {
    n = snprintf( &outbuf[ len ], bsz,
                     "\"verbose\":false,\"echo\":false,\"binary\":true}\r\n" );
    len += min_int( n, (int) bsz - 1 );
  }
  if ( nats_client_cmd_verbose )
    printf( "%.*s", len, outbuf );
  this->append( outbuf, len );
  /* if all subs are forwarded to NATS */
  if ( this->fwd_all_subs )
    this->sub_route.add_route_notify( *this );
  /* if all msgs are forwarded to NATS */
  if ( this->fwd_all_msgs ) {
    uint32_t h = this->sub_route.prefix_seed( 0 );
    this->sub_route.add_pattern_route( h, this->fd, 0 );
  }
  /* done, notify connected, may be -ERR later if conn fails to authenticate */
  if ( this->notify != NULL )
    this->notify->on_connect( *this );
}
/* after closed, release memory used by protocol */
void
EvNatsClient::release( void ) noexcept
{
  if ( this->fwd_all_msgs ) {
    uint32_t h = this->sub_route.prefix_seed( 0 );
    this->sub_route.del_pattern_route( h, this->fd, 0 );
  }
  if ( this->fwd_all_subs )
    this->sub_route.remove_route_notify( *this );
  this->release_fragments();
  if ( this->sid_ht != NULL ) {
    delete this->sid_ht;
    this->sid_ht = NULL;
  }
  this->pat_tab.release();
  if ( this->notify != NULL )
    this->notify->on_shutdown( *this, this->err, this->err_len );
  this->EvConnection::release_buffers();
}

