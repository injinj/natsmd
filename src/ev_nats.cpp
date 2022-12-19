#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#ifndef _MSC_VER
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#else
#include <raikv/win.h>
#endif
#include <natsmd/ev_nats.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <raikv/ev_publish.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <raikv/pattern_cvt.h>
#include <raimd/json_msg.h>

extern "C" {
const char *
natsmd_get_version( void )
{
  return kv_stringify( NATSMD_VER );
}
}
uint32_t rai::natsmd::nats_debug = 0;

using namespace rai;
using namespace natsmd;
using namespace kv;
using namespace md;

EvNatsListen::EvNatsListen( EvPoll &p ) noexcept
  : EvTcpListen( p, "nats_listen", "nats_sock" ), sub_route( p.sub_route ) {}

EvNatsListen::EvNatsListen( EvPoll &p,  RoutePublish &sr ) noexcept
  : EvTcpListen( p, "nats_listen", "nats_sock" ), sub_route( sr ) {}

int
EvNatsListen::listen( const char *ip,  int port,  int opts ) noexcept
{
  return this->kv::EvTcpListen::listen2( ip, port, opts, "nats_listen",
                                         this->sub_route.route_id );
}

/*
 * NATS protocol:
 *
 * 1. session init
 * server -> INFO { (below) } \r\n
 * (optional)
 * client -> CONNECT { verbose, pedantic, tls_required, auth_token, user,
 *                     pass, name, lang, protocol, echo } \r\n
 * 2. subscribe
 * client -> SUB <subject> [queue group] <sid> \r\n
 *        -> UNSUB <sid> [max-msgs] \r\n
 *
 * ping/pong, pong responds to ping, used for keepalive:
 * client/server -> PING \r\n
 *               -> PONG \r\n
 *
 * 3. publish
 * client -> PUB <subject> [reply-to] <#bytes> \r\n [payload] \r\n
 * server -> MSG <subject> <sid> [reply-to] <#bytes> \r\n [payload] \r\n
 *
 * 4. error / ok status (ok turned off by verbose=false)
 * server -> +OK \r\n
 * server -> -ERR (opt msg) \r\n
 */

/* ID = 22 chars base62 string dependent on the hash of the database = 255 */
static char nats_server_info[] =
"INFO {\"server_id\":\"______________________\","
      "\"version\":\"1.1.1\",\"go\":\"go1.5.0\","
      "\"host\":\"255.255.255.255\",\"port\":65535,"
      "\"auth_required\":false,\"ssl_required\":false,"
      "\"tls_required\":false,\"tls_verify\":false,"
      "\"max_payload\":1048576}\r\n";
bool is_server_info_init;

static void
init_server_info( uint64_t h1,  uint64_t h2,  uint16_t port )
{
  char host[ 256 ];
  struct addrinfo *res = NULL, *p;
  uint64_t r = 0;
  int i;
  rand::xorshift1024star prng;
  uint64_t svid[ 2 ] = { h1, h2 };

  prng.init( svid, sizeof( svid ) ); /* same server id until shm destroyed */

  for ( i = 0; i < 22; i++ ) {
    if ( ( i % 10 ) == 0 )
      r = prng.next();
    char c = r % 62;
    c = ( c < 10 ) ? ( c + '0' ) : (
        ( c < 36 ) ? ( ( c - 10 ) + 'A' ) : ( ( c - 36 ) + 'a' ) );
    nats_server_info[ 19 + i ] = c;
    r >>= 6;
  }

  if ( ::gethostname( host, sizeof( host ) ) == 0 &&
       ::getaddrinfo( host, NULL, NULL, &res ) == 0 ) {
    for ( p = res; p != NULL; p = p->ai_next ) {
      if ( p->ai_family == AF_INET && p->ai_addr != NULL ) {
        char *ip = ::inet_ntoa( ((struct sockaddr_in *) p->ai_addr)->sin_addr );
        size_t len = ::strlen( ip );
        ::memcpy( &nats_server_info[ 84 ], ip, len );
        nats_server_info[ 84 + len++ ] = '\"';
        nats_server_info[ 84 + len++ ] = ',';
        while ( len < 17 )
          nats_server_info[ 84 + len++ ] = ' ';
        break;
      }
    }
    ::freeaddrinfo( res );
  }

  for ( i = 0; port > 0; port /= 10 ) 
    nats_server_info[ 112 - i++ ] = ( port % 10 ) + '0';
  while ( i < 5 )
    nats_server_info[ 112 - i++ ] = ' ';
  is_server_info_init = true;
}

EvSocket *
EvNatsListen::accept( void ) noexcept
{
  EvNatsService *c =
    this->poll.get_free_list<EvNatsService, EvNatsListen &>(
      this->accept_sock_type, *this );

  if ( c == NULL )
    return NULL;
  if ( ! this->accept2( *c, "nats" ) )
    return NULL;

  if ( ! is_server_info_init ) {
    uint16_t port = 42222;
    uint64_t h1, h2;
    struct sockaddr_storage myaddr;
    socklen_t myaddrlen = sizeof( myaddr );
    int status;
#ifndef _MSC_VER
    status = ::getsockname( c->fd, (sockaddr *) &myaddr, &myaddrlen );
#else
    SOCKET sock;
    status = ::wp_get_socket( c->fd, &sock );
    if ( status == 0 )
      status = ::getsockname( sock, (sockaddr *) &myaddr, &myaddrlen );
#endif
    if ( status == 0 ) {
      if ( myaddr.ss_family == AF_INET )
        port = ntohs( ((sockaddr_in *) &myaddr)->sin_port );
      else if ( myaddr.ss_family == AF_INET6 )
        port = ntohs( ((sockaddr_in6 *) &myaddr)->sin6_port );
    }
    h1 = this->poll.create_ns();
    h2 = /*this->poll.map->hdr.seed[ 0 ].hash2;*/ 0;
    init_server_info( h1, h2, port );
  }
  c->initialize_state( NULL, 0 );
  c->append_iov( nats_server_info, sizeof( nats_server_info ) - 1 );
  c->idle_push( EV_WRITE_HI );
  return c;
}

void
EvNatsService::process( void ) noexcept
{
  enum { DO_OK = 1, DO_ERR = 2, NEED_MORE = 4, FLOW_BACKPRESSURE = 8,
         HAS_PING = 16 };
  static const char ok[]   = "+OK\r\n",
                    err[]  = "-ERR\r\n",
                    pong[] = "PONG\r\n";
  size_t            buflen, used, linesz, nargs, size_len, max_msgs;
  char            * p, * eol, * start, * end, * size_start;
  NatsArgs          args;
  int               fl, verb_ok/*, cmd_cnt, msg_cnt*/;

  for (;;) { 
    buflen = this->len - this->off;
    if ( buflen == 0 )
      goto break_loop;

    start = &this->recv[ this->off ];
    end   = &start[ buflen ];

    used    = 0;
    fl      = 0;
    /*cmd_cnt = 0;*/
    /*msg_cnt = 0;*/
    verb_ok = ( this->user.verbose ? DO_OK : 0 );
    /* decode nats hdrs */
    for ( p = start; p < end; ) {
      if ( this->msg_state == NATS_HDR_STATE ) {
        eol = (char *) ::memchr( &p[ 1 ], '\n', end - &p[ 1 ] );
        if ( eol != NULL ) {
          linesz = &eol[ 1 ] - p;
          if ( linesz > 3 ) {
            uint32_t kw = unaligned<uint32_t>( p ) & 0xdfdfdfdf; /* 4 toupper */
            if ( this->user.stamp == 0 ) {
              switch ( kw ) {
                case NATS_KW_SUB1:
                case NATS_KW_SUB2:
                case NATS_KW_PUB1:
                case NATS_KW_PUB2:
                case NATS_KW_UNSUB:
                case NATS_KW_PING: /* setup user if none specified */
                  this->parse_connect( NULL, 0 );
                  break;
                default:
                  break;
              }
            }
            switch ( kw ) {
            /*case NATS_KW_OK1:     // client side only
              case NATS_KW_OK2:     break;
              case NATS_KW_MSG1:   // MSG <subject> <sid> [reply] <size>
              case NATS_KW_MSG2:    break;
              case NATS_KW_ERR:     break; */
              case NATS_KW_SUB1:   /* SUB <subject> [queue group] <sid> */
              case NATS_KW_SUB2:
                nargs = args.parse( &p[ 4 ], eol );
                if ( nargs != 2 ) {
                  if ( nargs != 3 ) {
                    fl |= DO_ERR;
                    break;
                  }
                  /* SUB <subject> <queue> <sid> */
                  this->subject     = args.ptr[ 0 ];
                  this->queue       = args.ptr[ 1 ];
                  this->sid         = args.ptr[ 2 ];
                  this->subject_len = args.len[ 0 ];
                  this->queue_len   = args.len[ 1 ];
                  this->sid_len     = args.len[ 2 ];
                }
                else {
                  /* SUB <subject> <sid> */
                  this->subject     = args.ptr[ 0 ];
                  this->queue       = NULL;
                  this->sid         = args.ptr[ 1 ];
                  this->subject_len = args.len[ 0 ];
                  this->queue_len   = 0;
                  this->sid_len     = args.len[ 1 ];
                }
                this->add_sub();
                fl |= verb_ok;
                break;
              case NATS_KW_PUB1:   /* PUB <subject> [reply] <size> */
              case NATS_KW_PUB2:
                size_start = args.parse_end_size( p, eol - 1, this->msg_len,
                                                  size_len );
                if ( size_start == NULL ) {
                  fl |= DO_ERR;
                  break;
                }
                if ( linesz <= sizeof( this->buffer ) ) {
                  ::memcpy( this->buffer, p, linesz );
                  size_start = &this->buffer[ size_start - p ];
                  p = this->buffer;
                }
                else {
                  char *tmp = this->alloc( linesz );
                  ::memcpy( tmp, p, linesz );
                  size_start = &tmp[ size_start - p ];
                  p = tmp;
                }
                nargs = args.parse( &p[ 4 ], size_start );
                if ( nargs < 1 || nargs > 2 ) {
                  fl |= DO_ERR;
                  break;
                }
                this->subject     = args.ptr[ 0 ];
                this->subject_len = args.len[ 0 ];
                if ( nargs > 1 ) {
                  this->reply     = args.ptr[ 1 ];
                  this->reply_len = args.len[ 1 ];
                }
                else {
                  this->reply     = NULL;
                  this->reply_len = 0;
                }
                this->msg_state = NATS_PUB_STATE;
                break;
              case NATS_KW_PING:
                this->append( pong, sizeof( pong ) - 1 );
                fl |= HAS_PING;
                break;
            /*case NATS_KW_PONG:    break;
              case NATS_KW_INFO:    break;*/
              case NATS_KW_UNSUB: /* UNSUB <sid> [max-msgs] */
                nargs = args.parse( &p[ 6 ], eol );
                if ( nargs != 1 ) {
                  if ( nargs != 2 ) {
                    fl |= DO_ERR;
                    break;
                  }
                  args.parse_end_size( args.ptr[ 1 ],
                                       &args.ptr[ 1 ][ args.len[ 1 ] ],
                                       max_msgs, size_len );
                }
                else {
                  max_msgs = 0;
                }
                this->sid         = args.ptr[ 0 ];
                this->sid_len     = args.len[ 0 ];
                this->subject     = NULL;
                this->subject_len = 0;
                this->rem_sid( max_msgs );
                fl |= verb_ok;
                break;
              case NATS_KW_CONNECT:
                this->parse_connect( p, linesz );
                break;
              default:
                break;
            }
          }
          p = &eol[ 1 ];
          used += linesz;
          /*cmd_cnt++;*/
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
          if ( verb_ok == DO_OK )
            this->append( ok, sizeof( ok ) - 1 );
          if ( ! this->fwd_pub() )
            fl |= FLOW_BACKPRESSURE;
          /*fl |= verb_ok;*/
          /*msg_cnt++;*/
        }
        else { /* not enough to consume message */
          fl |= NEED_MORE;
        }
      }
      if ( ( fl & NEED_MORE ) != 0 ) {
        this->pushpop( EV_READ, EV_READ_LO );
        break;
      }
      if ( ( fl & DO_OK ) != 0 )
        this->append( ok, sizeof( ok ) - 1 );
      if ( ( fl & DO_ERR ) != 0 )
        this->append( err, sizeof( err ) - 1 );
      if ( ( fl & ( FLOW_BACKPRESSURE | HAS_PING ) ) != 0 ) {
        this->off += (uint32_t) used;
        if ( this->pending() > 0 )
          this->push( EV_WRITE_HI );
        if ( this->test( EV_READ ) )
          this->pushpop( EV_READ_LO, EV_READ );
        return;
      }
      fl = 0;
    }
    this->off += (uint32_t) used;
    if ( used == 0 || ( fl & NEED_MORE ) != 0 )
      goto break_loop;
  }
break_loop:;
  this->pop( EV_PROCESS );
  this->push_write();
  return;
}

uint8_t
EvNatsService::is_subscribed( const NotifySub &sub ) noexcept
{
  uint8_t v    = 0;
  bool    coll = false;
  if ( this->map.sub_tab.find3( sub.subj_hash, sub.subject, sub.subject_len,
                                coll ) == NATS_OK )
    v |= EV_SUBSCRIBED;
  else
    v |= EV_NOT_SUBSCRIBED;
  if ( coll )
    v |= EV_COLLISION;
  return v;
}

uint8_t
EvNatsService::is_psubscribed( const NotifyPattern &pat ) noexcept
{
  uint8_t v    = 0;
  bool    coll = false;
  const PatternCvt & cvt = pat.cvt;
  NatsPatternRoute * rt;
  if ( this->map.pat_tab.find3( pat.prefix_hash, pat.pattern, cvt.prefixlen,
                                rt, coll ) == NATS_OK ) {
    NatsWildMatch *m;
    for ( m = rt->list.hd; m != NULL; m = m->next ) {
      if ( m->len == pat.pattern_len &&
           ::memcmp( pat.pattern, m->value, m->len ) == 0 ) {
        v |= EV_SUBSCRIBED;
        break;
      }
    }
    if ( m == NULL )
      v |= EV_NOT_SUBSCRIBED | EV_COLLISION;
    else if ( rt->count > 1 )
      v |= EV_COLLISION;
  }
  else {
    v |= EV_NOT_SUBSCRIBED;
  }
  if ( coll )
    v |= EV_COLLISION;
  return v;
}

void
EvNatsService::add_sub( void ) noexcept
{
  const char * sub       = this->subject;
  size_t       sublen    = this->subject_len,
               preflen   = this->prefix_len;
  char       * inbox     = NULL;
  size_t       inbox_len = 0;

  if ( preflen > 0 ) {
    CatPtr tmp( this->alloc_temp( sublen + preflen ) );
    tmp.x( this->prefix, preflen ).x( sub, sublen );
    sub     = tmp.start;
    sublen += preflen;
  }

  NatsStr sid( this->sid, this->sid_len );
  NatsStr subj( sub, sublen );
  bool    coll = false;
  NatsSubStatus status;

  if ( is_nats_debug )
    printf( "add_sub %.*s sid %.*s\n", (int) sublen, sub,
            (int) this->sid_len, this->sid );

  if ( subj.is_wild() ) {
    PatternCvt cvt;
    if ( cvt.convert_rv( subj.str, subj.len ) != 0 )
      status = NATS_BAD_PATTERN;
    else {
      uint32_t h = kv_crc_c( subj.str, cvt.prefixlen,
                             this->sub_route.prefix_seed( cvt.prefixlen ) );
      NatsStr pre( subj.str, cvt.prefixlen, h );
      NatsWildMatch * sub_m = NULL;
      status = this->map.put_wild( subj, cvt, pre, sid, coll, sub_m );
      if ( status == NATS_IS_NEW || status == NATS_OK ||
           ( status == NATS_EXISTS && sub_m != NULL ) ) {
        NotifyPattern npat( cvt, subj.str, subj.len, h,
                            this->fd, coll, 'N', this );
        if ( status == NATS_IS_NEW )
          this->sub_route.add_pat( npat );
        else {
          npat.sub_count = sub_m->refcnt;
          this->sub_route.notify_pat( npat );
        }
      }
    }
  }
  else {
    if ( this->session_len > 0 ) {
      CatPtr ibx( this->alloc_temp( preflen + 7 + this->session_len + 1 +
                                    this->sid_len + 1 ) );
      ibx.x( this->prefix, preflen ).s( "_INBOX." )
         .x( this->session, this->session_len ).c( '.' )
         .x( this->sid, this->sid_len ).end();

      inbox     = ibx.start;
      inbox_len = ibx.len();
    }

    NatsSubRoute * sub_rt = NULL;
    status = this->map.put( subj, sid, coll, sub_rt );
    if ( status == NATS_IS_NEW || status == NATS_OK ||
         ( status == NATS_EXISTS && sub_rt != NULL ) ) {
      NotifySub nsub( subj.str, subj.len, inbox, inbox_len, subj.hash(),
                      this->fd, coll, 'N', this );
      if ( status == NATS_IS_NEW )
        this->sub_route.add_sub( nsub );
      else {
        nsub.sub_count = sub_rt->refcnt;
        this->sub_route.notify_sub( nsub );
      }
    }
  }
  if ( status > NATS_EXISTS ) {
    fprintf( stderr, "add_sub( %.*s, %.*s ) = %s\n",
             subj.len, subj.str, sid.len, sid.str, nats_status_str( status ) );
  }
}

void
EvNatsService::rem_sid( uint64_t max_msgs ) noexcept
{
  NatsStr       sid( this->sid, this->sid_len );
  NatsLookup    look;
  NatsSubStatus status;
  bool          coll = false;

  status = map.unsub( sid, max_msgs, look, coll );
  if ( status == NATS_EXPIRED ) {
    if ( look.rt != NULL ) {
      NotifySub nsub( look.rt->value, look.rt->subj_len, look.hash, this->fd,
                      coll, 'N', this );
      this->sub_route.del_sub( nsub );
    }
    else {
      PatternCvt cvt;
      if ( cvt.convert_rv( look.match->value, look.match->subj_len ) == 0 ) {
        NotifyPattern npat( cvt, look.match->value, look.match->subj_len,
                            look.hash, this->fd, coll, 'N', this );
        this->sub_route.del_pat( npat );
      }
    }
    map.unsub_remove( look );
  }
}

void
EvNatsService::rem_all_sub( void ) noexcept
{
  RouteLoc           loc;
  NatsSubRoute     * r;
  NatsPatternRoute * p;

  for ( r = this->map.sub_tab.first( loc ); r;
        r = this->map.sub_tab.next( loc ) ) {
    bool coll = this->map.sub_tab.rem_collision( r );
    NotifySub nsub( r->value, r->subj_len, r->hash, this->fd, coll, 'N', this );
    this->sub_route.del_sub( nsub );
  }
  for ( p = this->map.pat_tab.first( loc ); p;
        p = this->map.pat_tab.next( loc ) ) {
    for ( NatsWildMatch *m = p->list.hd; m != NULL; m = m->next ) {
      PatternCvt cvt;
      if ( cvt.convert_rv( m->value, m->subj_len ) == 0 ) {
        bool coll = this->map.pat_tab.rem_collision( p, m );
        NotifyPattern npat( cvt, m->value, m->subj_len, p->hash,
                            this->fd, coll, 'N', this );
        this->sub_route.del_pat( npat );
      }
    }
  }
}

bool
EvNatsService::fwd_pub( void ) noexcept
{
  size_t    preflen = this->prefix_len;
  const char  * sub = this->subject,
              * rep = this->reply;
  size_t     sublen = this->subject_len,
             replen = this->reply_len;
  if ( preflen > 0 ) {
    CatPtr tmp( this->alloc_temp( sublen + preflen ) );
    tmp.x( this->prefix, preflen ).x( sub, sublen );
    sub     = tmp.start;
    sublen += preflen;
    if ( replen > 0 ) {
      CatPtr tmp( this->alloc_temp( replen + preflen ) );
      tmp.x( this->prefix, preflen ).x( rep, replen );
      rep     = tmp.start;
      replen += preflen;
    }
  }
  uint32_t  h = kv_crc_c( sub, sublen, 0 );
  EvPublish pub( sub, sublen, rep, replen,
                 this->msg_ptr, this->msg_len,
                 this->sub_route, this->fd, h,
                 MD_STRING, 'p' );
  return this->sub_route.forward_msg( pub );
}

bool
EvNatsService::on_msg( EvPublish &pub ) noexcept
{
  NatsStr          subj, sid, pre;
  NatsLookup       look;
  NatsMsgTransform xf( pub, sid );
  NatsSubStatus    status;
  bool             b, coll, flow_good = true;

  /* if client does not want to see the msgs it published */
  if ( ! this->user.echo && (uint32_t) this->fd == pub.src_route )
    return true;

  for ( uint8_t cnt = 0; cnt < pub.prefix_cnt; cnt++ ) {
    uint32_t h = pub.hash[ cnt ];
    if ( pub.subj_hash == h ) {
      subj.set( pub.subject, pub.subject_len, h );
      status = this->map.lookup_publish( subj, look );
      if ( status != NATS_NOT_FOUND ) { /* OK or EXPIRED */
        for ( b = look.rt->first_sid( sid ); b; b = look.rt->next_sid( sid ) ) {
          flow_good &= this->fwd_msg( pub, xf );
        }
        if ( status == NATS_EXPIRED ) {
          status = this->map.expired( look, coll );
          if ( status == NATS_EXPIRED ) {
            NotifySub nsub( subj.str, subj.len, h, this->fd, coll, 'N', this );
            this->sub_route.del_sub( nsub );
            this->map.unsub_remove( look );
          }
        }
      }
    }
    else {
      pre.set( pub.subject, pub.prefix[ cnt ], h );
      subj.set( pub.subject, pub.subject_len );
      status = this->map.lookup_pattern( pre, subj, look );

      for (;;) {
        if ( status == NATS_NOT_FOUND )
          break;
        for ( b = look.match->first_sid( sid ); b;
              b = look.match->next_sid( sid ) ) {
          if ( sid.len == 1 && sid.str[ 0 ] == 'I' )
            return this->on_inbox_reply( pub );
          flow_good &= this->fwd_msg( pub, xf );
        }
        if ( status == NATS_EXPIRED ) {
          status = this->map.expired_pattern( look, coll );
          if ( status == NATS_EXPIRED ) {
            PatternCvt cvt;
            if ( cvt.convert_rv( look.match->value,
                                 look.match->subj_len ) == 0 ) {
              NotifyPattern npat( cvt, look.match->value, look.match->subj_len,
                                  h, this->fd, coll, 'N', this );
              this->sub_route.del_pat( npat );
            }
            this->map.unsub_remove( look );
            break;
          }
        }
        status = this->map.lookup_next( subj, look );
      }
    }
  }
  return flow_good;
}

bool
EvNatsService::on_inbox_reply( EvPublish &pub ) noexcept
{
  /* if inbox reply */
  const char * sub = pub.subject;
  size_t       off = pub.subject_len;
  while ( off > 0 && sub[ off - 1 ] != '.' )
    off--;
  const char * sid     = &sub[ off ];
  size_t       sid_len = pub.subject_len - off;
  NatsStr sid2( sid, sid_len );
  NatsLookup look;
  if ( this->map.find_by_sid( sid2, look ) == NATS_OK &&
       look.rt != NULL ) {
    EvPublish pub2( pub );
    MDMsgMem  tmp;
    uint32_t  tmp_hash[ 1 ];
    size_t    len = look.rt->subj_len;
    char    * sub = tmp.str_make( len  );

    ::memcpy( sub, look.rt->value, len );
    pub2.subject_len = len;
    pub2.subject     = sub;
    pub2.subj_hash   = look.rt->hash;
    pub2.hash        = tmp_hash;
    pub2.prefix_cnt  = 1;
    tmp_hash[ 0 ]    = pub2.subj_hash;
    return this->on_msg( pub2 );
  }
  return true;
}

bool
EvNatsService::hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept
{
  NatsSubRoute * rt;
  if ( (rt = this->map.sub_tab.find_by_hash( h )) != NULL ) {
    ::memcpy( key, rt->value, rt->subj_len );
    keylen = rt->subj_len;
    return true;
  }
  return false;
}

bool
EvNatsService::fwd_msg( EvPublish &pub,  NatsMsgTransform &xf ) noexcept
{
  const char  * sid  = xf.sid.str;
  size_t    sid_len  = xf.sid.len;
  const char  * sub  = pub.subject,
              * rep  = (const char *) pub.reply;
  size_t     sublen  = pub.subject_len,
             replen  = pub.reply_len,
             preflen = this->prefix_len;

  if ( sublen < preflen ) {
    fprintf( stderr, "sub %.*s is less than prefix (%u)\n",
             (int) sublen, sub, (int) preflen );
    return true;
  }
  if ( replen != 0 && replen < preflen ) {
    fprintf( stderr, "rep %.*s is less than prefix (%u)\n",
             (int) replen, rep, (int) preflen );
    return true;
  }
  sublen -= preflen;
  sub     = &sub[ preflen ];
  if ( replen > 0 ) {
    replen -= preflen;
    rep     = &rep[ preflen ];
  }
  if ( ! xf.is_ready ) {
    xf.is_ready = true;
    if ( pub.pub_status != EV_PUB_NORMAL ) {
      /* start and cycle are normal events */
      if ( pub.pub_status <= EV_MAX_LOSS || pub.pub_status == EV_PUB_RESTART ) {
        if ( this->notify != NULL )
          this->notify->on_data_loss( *this, pub );
      }
    }
    xf.check_transform();
  }
  size_t msg_len_digits = uint64_digits( xf.msg_len );
  size_t len = 4 +                    /* MSG */
               sublen + 1 +           /* <subject> */
               sid_len + 1 +          /* <sid> */
    ( replen > 0 ? replen + 1 : 0 ) + /* [reply] */
               msg_len_digits + 2 +   /* <size> \r\n */
               xf.msg_len + 2;        /* <blob> \r\n */
  CatPtr p( this->alloc( len ) );

  p.s( "MSG " ).x( sub, sublen ).c( ' ' )
               .x( sid, sid_len ).c( ' ' );
  if ( replen > 0 )
    p.x( rep, replen ).c( ' ' );
  p.u( xf.msg_len, msg_len_digits ).s( "\r\n" )
   .b( xf.msg, xf.msg_len ).s( "\r\n" );

  this->sz += len;
  return this->idle_push_write();
}

void
NatsMsgTransform::transform( void ) noexcept
{
  MDMsg * m = MDMsg::unpack( (void *) this->msg, 0, this->msg_len, 0,
                             NULL, &this->spc );
  if ( m == NULL )
    return;

  size_t max_len = ( ( this->msg_len | 15 ) + 1 ) * 16;
  char * start = this->spc.str_make( max_len );
  JsonMsgWriter jmsg( start, max_len );
  if ( jmsg.convert_msg( *m ) == 0 && jmsg.finish() ) {
    this->msg     = start;
    this->msg_len = jmsg.off;
  }
}

void
EvNatsService::process_close( void ) noexcept
{
  this->client_stats( this->sub_route.peer_stats );
  this->EvSocket::process_close();
}

void
EvNatsService::release( void ) noexcept
{
  //printf( "nats release fd=%d\n", this->fd );
  this->rem_all_sub();
  this->map.release();
  this->EvConnection::release_buffers();
  if ( this->notify != NULL )
    this->notify->on_shutdown( *this, NULL, 0 );
  this->user.release();
}

bool
EvNatsService::timer_expire( uint64_t, uint64_t ) noexcept
{
  return false;
}

size_t
EvNatsService::get_userid( char userid[ MAX_USERID_LEN ] ) noexcept
{
  if ( this->user.user != NULL ) {
    size_t len = min_int( MAX_USERID_LEN - 1, ::strlen( this->user.user ) );
    ::memcpy( userid, this->user.user, len );
    return len;
  }
  userid[ 0 ] = '\0';
  return 0;
}

void
EvNatsService::set_session( const char *sess,  size_t sess_len ) noexcept
{
  if ( sess_len == 0 || sess_len >= sizeof( this->session ) ) {
    fprintf( stderr, "bad session_len %lu\n", sess_len );
    this->session_len = 0;
    return;
  }
  ::memcpy( this->session, sess, sess_len );
  this->session[ sess_len ] = '\0';
  this->session_len = sess_len;

  static char inbox_sid[] = "I";
  CatBuf< 7 + sizeof( this->session ) + 3 > inbox;

  inbox.s( "_INBOX." ).x( sess, sess_len ).s( ".>" ).end();

  this->subject     = inbox.buf;
  this->subject_len = inbox.len();
  this->sid         = inbox_sid;
  this->sid_len     = 1;
  this->reply       = NULL;
  this->reply_len   = 0;
  this->queue       = NULL;
  this->queue_len   = 0;

  this->add_sub();
}

size_t
EvNatsService::get_session( const char *svc,  size_t svc_len,
                            char session[ MAX_SESSION_LEN ] ) noexcept
{
  if ( this->session_len > 0 ) {
    bool match = ( svc_len == 0 ||
                   ( svc_len == this->prefix_len &&
                     ::memcmp( svc, this->prefix, svc_len ) == 0 ) );
    if ( match ) {
      ::memcpy( session, this->session, this->session_len );
      session[ this->session_len ] = '\0';
      return this->session_len;
    }
  }
  session[ 0 ] = '\0';
  return 0;
}

size_t
EvNatsService::get_subscriptions( SubRouteDB &subs,  SubRouteDB &pats,
                                  int &pattern_fmt ) noexcept
{
  RouteLoc           pos,
                     loc;
  NatsSubRoute     * r;
  NatsPatternRoute * p;
  size_t             prelen = this->prefix_len,
                     cnt    = 0,
                     len;
  const char       * val;
  uint32_t           h;

  pattern_fmt = RV_PATTERN_FMT;
  for ( r = this->map.sub_tab.first( pos ); r != NULL;
        r = this->map.sub_tab.next( pos ) ) {
    val = &r->value[ prelen ];
    len = r->subj_len - prelen;
    h   = kv_crc_c( val, len, 0 );
    subs.upsert( h, val, len, loc );
    if ( loc.is_new )
      cnt++;
  }
  for ( p = this->map.pat_tab.first( pos ); p != NULL;
        p = this->map.pat_tab.next( pos ) ) {
    for ( NatsWildMatch *m = p->list.hd; m != NULL; m = m->next ) {
      val = &m->value[ prelen ];
      len = m->subj_len - prelen;
      h   = kv_crc_c( val, len, 0 );
      pats.upsert( h, val, len, loc );
      if ( loc.is_new )
        cnt++;
    }
  }
  return cnt;
}


void
EvNatsService::parse_connect( const char *buf,  size_t bufsz ) noexcept
{
  const char  * start,
              * end;
  MDMsgMem      mem;
  JsonMsg     * msg;
  MDFieldIter * iter;
  MDName        name;
  MDReference   mref;

  if ( bufsz == 0 )
    goto do_notify;
  if ( (start = (const char *) ::memchr( buf, '{', bufsz )) == NULL )
    goto do_notify;
  bufsz -= start - buf;
  for ( end = &start[ bufsz ]; end > start; )
    if ( *--end == '}' )
      break;

  if ( end <= start )
    goto do_notify;

  msg = JsonMsg::unpack( (void *) start, 0, &end[ 1 ] - start, 0, NULL, &mem );
  if ( msg == NULL )
    goto do_notify;
  if ( msg->get_field_iter( iter ) != 0 )
    goto do_notify;
  if ( iter->first() != 0 )
    goto do_notify;

  do {
    if ( iter->get_name( name ) == 0 ) {
      if ( name.fnamelen <= 4 ) /* go:"str" */
        continue;

      switch ( ( unaligned<uint32_t>( name.fname ) ) & 0xdfdfdfdf ) {
        case NATS_JS_VERBOSE: /* verbose:false */
          if ( iter->get_reference( mref ) == 0 && mref.ftype == MD_BOOLEAN )
            this->user.verbose = ( mref.fptr[ 0 ] != 0 );
          break;
        case NATS_JS_PEDANTIC: /* pedantic:false */
          if ( iter->get_reference( mref ) == 0 && mref.ftype == MD_BOOLEAN )
            this->user.pedantic = ( mref.fptr[ 0 ] != 0 );
          break;
        case NATS_JS_TLS_REQUIRE: /* tls_require:false */
          if ( iter->get_reference( mref ) == 0 && mref.ftype == MD_BOOLEAN )
            this->user.tls_require = ( mref.fptr[ 0 ] != 0 );
          break;
        case NATS_JS_ECHO: /* echo:false */
          if ( iter->get_reference( mref ) == 0 && mref.ftype == MD_BOOLEAN )
            this->user.echo = ( mref.fptr[ 0 ] != 0 );
          break;
        case NATS_JS_PROTOCOL: /* proto:1 */
          if ( iter->get_reference( mref ) == 0 )
            cvt_number( mref, this->user.protocol );
          break;
        case NATS_JS_NAME: /* name:"str" */
          if ( iter->get_reference( mref ) == 0 && mref.ftype == MD_STRING )
            this->user.save_string( this->user.name, mref.fptr, mref.fsize );
          break;
        case NATS_JS_LANG: /* lang:"C" */
          if ( iter->get_reference( mref ) == 0 && mref.ftype == MD_STRING )
            this->user.save_string( this->user.lang, mref.fptr, mref.fsize );
          break;
        case NATS_JS_VERSION: /* version:"str" */
          if ( iter->get_reference( mref ) == 0 && mref.ftype == MD_STRING )
            this->user.save_string( this->user.version, mref.fptr, mref.fsize );
          break;
        case NATS_JS_USER: /* user:"str" */
          if ( iter->get_reference( mref ) == 0 && mref.ftype == MD_STRING )
            this->user.save_string( this->user.user, mref.fptr, mref.fsize );
          break;
        case NATS_JS_PASS: /* pass:"str" */
          if ( iter->get_reference( mref ) == 0 && mref.ftype == MD_STRING )
            this->user.save_string( this->user.pass, mref.fptr, mref.fsize );
          break;
        case NATS_JS_AUTH_TOKEN: /* auth_token:"str" */
          if ( iter->get_reference( mref ) == 0 && mref.ftype == MD_STRING )
            this->user.save_string( this->user.auth_token, mref.fptr,
                                    mref.fsize );
          break;
        default:
          break;
      }
    }
  } while ( iter->next() == 0 );
do_notify:;
  if ( this->user.user == NULL || ::strlen( this->user.user ) == 0 )
    this->user.save_string( this->user.user, "nobody", 6 );
  if ( this->user.stamp == 0 ) {
    this->user.stamp = this->active_ns;
    if ( this->notify != NULL )
      this->notify->on_connect( *this );
  }
}

NatsWildMatch *
NatsWildMatch::create( NatsStr &subj,  NatsStr &sid,
                       kv::PatternCvt &cvt ) noexcept
{
  pcre2_real_code_8       * re = NULL;
  pcre2_real_match_data_8 * md = NULL;
  size_t erroff;
  int    error;
  bool   pattern_success = false;
  /* if prefix matches, no need for pcre2 */
  if ( cvt.prefixlen + 1 == subj.len && subj.str[ cvt.prefixlen ] == '>' )
    pattern_success = true;
  else {
    re = pcre2_compile( (uint8_t *) cvt.out, cvt.off, 0, &error,
                        &erroff, 0 );
    if ( re == NULL ) {
      fprintf( stderr, "re failed\n" );
    }
    else {
      md = pcre2_match_data_create_from_pattern( re, NULL );
      if ( md == NULL )
        fprintf( stderr, "md failed\n" );
      else
        pattern_success = true;
    }
  }
  if ( pattern_success ) {
    size_t sz = sizeof( NatsWildMatch ) + subj.len + sid.len + 2 - 2;
    void * p  = ::malloc( sz );
    if ( p != NULL )
      return new ( p ) NatsWildMatch( subj, sid, re, md );
  }
  if ( md != NULL )
    pcre2_match_data_free( md );
  if ( re != NULL )
    pcre2_code_free( re );
  return NULL;
}

NatsWildMatch::~NatsWildMatch()
{
  if ( this->md != NULL )
    pcre2_match_data_free( this->md );
  if ( this->re != NULL )
    pcre2_code_free( this->re );
}

NatsWildMatch *
NatsWildMatch::resize_sid( NatsWildMatch *m,  NatsStr &sid ) noexcept
{
  size_t sz = (size_t) m->sid_off + (size_t) sid.len + 2;
  if ( sz > 0xffffU )
    return NULL;
  void * p = ::realloc( (void *) m, sizeof( NatsWildMatch ) + sz - 2 );
  if ( p == NULL )
    return NULL;
  m = (NatsWildMatch *) p;
  m->len += sid.len + 2;
  m->add_sid( sid );
  return m;
}

bool
NatsWildMatch::match( NatsStr &subj ) noexcept
{
  return ( pcre2_match( this->re, (const uint8_t *) subj.str,
                        subj.len, 0, 0, this->md, 0 ) == 1 );
}

void
SidEntry::print( void ) noexcept
{
  printf( "%.*s", this->len, this->value );
  if ( this->max_msgs != 0 )
    printf( "[cnt=%" PRIu64 ",max=%" PRIu64 "]", this->msg_cnt, this->max_msgs);
  if ( this->pref_hash != 0 )
    printf( "[pattern]" );
  printf( "\n" );
}

void
NatsSubData::print_sids( void ) noexcept
{
  NatsStr sid;
  bool b;
  printf( "[refs=%u][cnt=%" PRIu64 "]", this->refcnt, this->msg_cnt );
  if ( this->max_msgs != 0 )
    printf( "[max=%" PRIu64 "]", this->max_msgs );
  printf( ":" );
  for ( b = this->first_sid( sid ); b; b = this->next_sid( sid ) ) {
    printf( " %.*s", sid.len, sid.str );
  }
  printf( "\n" );
}

void
NatsSubRoute::print( void ) noexcept
{
  printf( "%.*s", this->len, this->value );
  this->print_sids();
}

void
NatsPatternRoute::print( void ) noexcept
{
  NatsWildMatch * w;
  for ( w = this->list.hd; w; w = w->next ) {
    w->print();
    w->print_sids();
  }
}

void
NatsWildMatch::print( void ) noexcept
{
  printf( "%.*s", this->len, this->value );
}

void
NatsSubMap::print( void ) noexcept
{
  RouteLoc           loc;
  SidEntry         * s;
  NatsSubRoute     * r;
  NatsPatternRoute * p;

  printf( "-- sids:\n" );
  for ( s = this->sid_tab.first( loc ); s; s = this->sid_tab.next( loc ) ) {
    s->print();
  }
  printf( "-- subs:\n" );
  for ( r = this->sub_tab.first( loc ); r; r = this->sub_tab.next( loc ) ) {
    r->print();
  }
  printf( "-- patterns:\n" );
  for ( p = this->pat_tab.first( loc ); p; p = this->pat_tab.next( loc ) ) {
    p->print();
  }
}

const char *
rai::natsmd::nats_status_str( NatsSubStatus status )
{
  switch ( status ) {
    case NATS_OK          : return "nats_ok";
    case NATS_IS_NEW      : return "nats_is_new";
    case NATS_EXPIRED     : return "nats_expired";
    case NATS_NOT_FOUND   : return "nats_not_found";
    case NATS_EXISTS      : return "nats_exists";
    case NATS_TOO_MANY    : return "nats_too_many";
    case NATS_BAD_PATTERN : return "nats_bad_pattern";
    default: return "??";
  }
}

