#ifndef __rai_natsmd__ev_nats_h__
#define __rai_natsmd__ev_nats_h__

#include <raikv/ev_tcp.h>
#include <raikv/ev_publish.h>
#include <natsmd/nats_map.h>
#include <raimd/md_msg.h>

extern "C" {
const char *natsmd_get_version( void );
}
namespace rai {
namespace natsmd {

struct NatsLogin {
  uint64_t stamp;       /* time of login */
  bool     verbose,     /* whether +OK is sent */
           pedantic,    /* whether subject is checked for validity */
           tls_require, /* whether need TLS layer */
           echo;        /* whether to forward pubs to subs owned by client
                         (eg. multicast loopback) */
  int      protocol;    /* == 1 */
  char   * name,        /* connect parameters user:"str" */
         * lang,        /*                    lang:"C" */
         * version,     /*                    version:"1.1" */
         * user,        /*                    user:"str" */
         * pass,        /*                    pass:"str" */
         * auth_token;  /*                    auth_token:"str" */
  NatsLogin() {
    ::memset( (void *) this, 0, sizeof( *this ) );
    this->protocol = 1;
    this->verbose = true;
  }
  void release( void ) {
    for ( char ** i = &this->name; i <= &this->auth_token; i++ ) {
      if ( *i != NULL )
        ::free( *i );
    }
    ::memset( (void *) this, 0, sizeof( *this ) );
    this->protocol = 1;
    this->verbose = true;
  }
  static void save_string( char *&field,  const void *s,  size_t len ) {
    field = (char *) ::realloc( field, len + 1 );
    ::memcpy( field, s, len );
    field[ len ] = '\0';
  }
};

struct EvNatsListen : public kv::EvTcpListen {
  void * operator new( size_t, void *ptr ) { return ptr; }
  kv::RoutePublish & sub_route;

  EvNatsListen( kv::EvPoll &p,  kv::RoutePublish &sr ) noexcept;
  EvNatsListen( kv::EvPoll &p ) noexcept;

  virtual kv::EvSocket *accept( void ) noexcept;
  virtual int listen( const char *ip,  int port,  int opts ) noexcept;
};

struct EvPrefetchQueue;

enum NatsState {
  NATS_HDR_STATE = 0, /* parsing header opcode */
  NATS_PUB_STATE = 1  /* parsing PUB message bytes */
};

struct NatsMsgTransform {
  md::MDMsgMem spc;
  const void * msg;
  uint32_t     msg_len,
               msg_enc;
  NatsStr    & sid;
  bool         is_ready;

  NatsMsgTransform( kv::EvPublish &pub,  NatsStr &id )
    : msg( pub.msg ), msg_len( pub.msg_len ), msg_enc( pub.msg_enc ),
      sid( id ), is_ready( false ) {}

  void check_transform( void ) {
    if ( ! this->is_ready ) {
      if ( this->msg_len == 0 || this->msg_enc == md::MD_STRING ) {
        this->is_ready = true;
        return;
      }
      this->transform();
      this->is_ready = true;
    }
  }
  void transform( void ) noexcept;
};

struct EvNatsService : public kv::EvConnection {
  void * operator new( size_t, void *ptr ) { return ptr; }
  kv::RoutePublish & sub_route;
  EvNatsListen     & listen;

  NatsSubMap map;
  char     * msg_ptr;     /* ptr to the msg blob */
  size_t     msg_len;     /* size of the current message blob */
  NatsState  msg_state;   /* ascii hdr mode or binary blob mode */
  char     * subject;     /* either pub or sub subject w/opt reply: */
  size_t     subject_len; /* PUB <subject> <reply> */
  char     * reply;       /* SUB <subject> <sid> */
  size_t     reply_len;   /* len of reply */
  char     * sid;         /* <sid> of SUB */
  size_t     sid_len;     /* len of sid */
  char     * queue;
  size_t     queue_len;

  NatsLogin user;
  char      prefix[ 16 ];
  char      buffer[ 256 ];
  uint16_t  prefix_len,
            session_len;
  char      session[ MAX_SESSION_LEN ];

  EvNatsService( kv::EvPoll &p,  const uint8_t t,  EvNatsListen &l )
    : kv::EvConnection( p, t ), sub_route( l.sub_route ), listen( l ) {}
  void initialize_state( const char *pre,  size_t prelen ) {
    this->msg_ptr   = NULL;
    this->msg_len   = 0;
    this->msg_state = NATS_HDR_STATE;

    this->subject = this->reply = this->sid = this->queue = NULL;
    this->subject_len = this->reply_len = this->sid_len = this->queue_len = 0;

    this->user.release();
    this->prefix_len = prelen;
    ::memcpy( this->prefix, pre, prelen );
    this->session_len = 0;
  }
  void set_session( const char *sess,  size_t sess_len ) noexcept;
  void add_sub( void ) noexcept;
  void rem_sid( uint64_t max_msgs ) noexcept;
  void rem_all_sub( void ) noexcept;
  bool fwd_pub( void ) noexcept;
  bool fwd_msg( kv::EvPublish &pub,  NatsMsgTransform &xf ) noexcept;
  void parse_connect( const char *buf,  size_t sz ) noexcept;
  bool on_inbox_reply( kv::EvPublish &pub ) noexcept;
  /* EvSocket */
  virtual void process( void ) noexcept;
  virtual void process_close( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept;
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept;
  virtual uint8_t is_subscribed( const kv::NotifySub &sub ) noexcept;
  virtual uint8_t is_psubscribed( const kv::NotifyPattern &pat ) noexcept;
  virtual size_t get_userid( char userid[ MAX_USERID_LEN ] ) noexcept;
  virtual size_t get_session( const char *svc,  size_t svc_len,
                              char session[ MAX_SESSION_LEN ] ) noexcept;
  virtual size_t get_subscriptions( kv::SubRouteDB &subs,
                                    kv::SubRouteDB &pats,
                                    int &pattern_fmt ) noexcept;
};

/* presumes little endian, 0xdf masks out 0x20 for toupper() */
#define NATS_KW( c1, c2, c3, c4 ) ( (uint32_t) ( c4 & 0xdf ) << 24 ) | \
                                  ( (uint32_t) ( c3 & 0xdf ) << 16 ) | \
                                  ( (uint32_t) ( c2 & 0xdf ) << 8 ) | \
                                  ( (uint32_t) ( c1 & 0xdf ) )
#define NATS_KW_OK1     NATS_KW( '+', 'O', 'K', '\r' )
#define NATS_KW_OK2     NATS_KW( '+', 'O', 'K', '\n' )
#define NATS_KW_MSG1    NATS_KW( 'M', 'S', 'G', ' ' )
#define NATS_KW_MSG2    NATS_KW( 'M', 'S', 'G', '\t' )
#define NATS_KW_ERR     NATS_KW( '-', 'E', 'R', 'R' )
#define NATS_KW_SUB1    NATS_KW( 'S', 'U', 'B', ' ' )
#define NATS_KW_SUB2    NATS_KW( 'S', 'U', 'B', '\t' )
#define NATS_KW_PUB1    NATS_KW( 'P', 'U', 'B', ' ' )
#define NATS_KW_PUB2    NATS_KW( 'P', 'U', 'B', '\t' )
#define NATS_KW_PING    NATS_KW( 'P', 'I', 'N', 'G' )
#define NATS_KW_PONG    NATS_KW( 'P', 'O', 'N', 'G' )
#define NATS_KW_INFO    NATS_KW( 'I', 'N', 'F', 'O' )
#define NATS_KW_UNSUB   NATS_KW( 'U', 'N', 'S', 'U' )
#define NATS_KW_CONNECT NATS_KW( 'C', 'O', 'N', 'N' )

/* SUB1  = 4347219, 0x425553;  SUB2 = 155342163, 0x9425553 */
/* PUB1  = 4347216, 0x425550;  PUB2 = 155342160, 0x9425550 */
/* PING  = 1196312912, 0x474e4950 */
/* UNSUB = 1431522901, 0x55534e55 */

#define NATS_JS_VERBOSE      NATS_KW( 'V', 'E', 'R', 'B' )
#define NATS_JS_PEDANTIC     NATS_KW( 'P', 'E', 'D', 'A' )
#define NATS_JS_TLS_REQUIRE  NATS_KW( 'T', 'L', 'S', '_' )
#define NATS_JS_NAME         NATS_KW( 'N', 'A', 'M', 'E' )
#define NATS_JS_LANG         NATS_KW( 'L', 'A', 'N', 'G' )
#define NATS_JS_VERSION      NATS_KW( 'V', 'E', 'R', 'S' )
#define NATS_JS_PROTOCOL     NATS_KW( 'P', 'R', 'O', 'T' )
#define NATS_JS_ECHO         NATS_KW( 'E', 'C', 'H', 'O' )
#define NATS_JS_USER         NATS_KW( 'U', 'S', 'E', 'R' )
#define NATS_JS_PASS         NATS_KW( 'P', 'A', 'S', 'S' )
#define NATS_JS_AUTH_TOKEN   NATS_KW( 'A', 'U', 'T', 'H' )

#define NATS_JS_SERVER       NATS_KW( 'S', 'E', 'R', 'V' )
#define NATS_JS_MAX_PAYLOAD  NATS_KW( 'M', 'A', 'X', '_' )
#define NATS_JS_CONNECT_URLS NATS_KW( 'C', 'O', 'N', 'N' )
#define NATS_JS_CLIENT       NATS_KW( 'C', 'L', 'I', 'E' )
#define NATS_JS_HOST         NATS_KW( 'H', 'O', 'S', 'T' )
#define NATS_JS_PORT         NATS_KW( 'P', 'O', 'R', 'T' )
#define NATS_JS_GIT_COMMIT   NATS_KW( 'G', 'I', 'T', '_' )

struct NatsArgs {
  static const size_t MAX_NATS_ARGS = 3; /* <subject> <sid> [reply] */
  char * ptr[ MAX_NATS_ARGS ];
  size_t len[ MAX_NATS_ARGS ];

  /* parse args into ptr[], len[], return arg count */
  size_t parse( char *start,  char *end ) {
    char *p;
    size_t n;
    for ( p = start; ; p++ ) { /* skip PUB / MSG / SUB */
      if ( p >= end )
        return 0;
      if ( *p > ' ' )
        break;
    }
    n = 0;
    this->ptr[ 0 ] = p; /* break args up by spaces */
    for (;;) {
      if ( ++p == end || *p <= ' ' ) {
        this->len[ n ] = p - this->ptr[ n ];
        if ( ++n == MAX_NATS_ARGS )
          return n;
        for (;;) {
          if ( p >= end )
            return n;
          if ( *p > ' ' )
            break;
          p++;
        }
        this->ptr[ n ] = p;
      }
    }
  }
  /* parse a number from the end of a string */
  static char * parse_end_size( char *start,  char *end,  size_t &sz,
                                size_t &digits ) {
    while ( end > start ) {
      if ( *--end >= '0' && *end <= '9' ) {
        char *last_digit = end;
        sz = (size_t) ( *end - '0' );
        if ( *--end >= '0' && *end <= '9' ) {
          sz += (size_t) ( *end - '0' ) * 10;
          if ( *--end >= '0' && *end <= '9' ) {
            sz += (size_t) ( *end - '0' ) * 100;
            if ( *--end >= '0' && *end <= '9' ) {
              sz += (size_t) ( *end - '0' ) * 1000;
              if ( *--end >= '0' && *end <= '9' ) {
                size_t p = 10000;
                do {
                  sz += (size_t) ( *end - '0' ) * p;
                  p  *= 10;
                } while ( *--end >= '0' && *end <= '9' );
              }
            }
          }
        }
        digits = (size_t) ( last_digit - end );
        return end;
      }
    }
    sz = 0;
    digits = 0;
    return NULL;
  }
};

#define is_nats_debug kv_unlikely( nats_debug != 0 )
extern uint32_t nats_debug;

}
}
#endif
