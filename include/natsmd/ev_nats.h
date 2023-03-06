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
  uint64_t stamp;         /* time of login */
  bool     verbose,       /* whether +OK is sent */
           pedantic,      /* whether subject is checked for validity */
           tls_require,   /* whether need TLS layer */
           echo,          /* whether to forward pubs to subs owned by client */
           headers,       /* */
           no_responders, /* */
           binary;
  int      protocol;      /* == 1 */
  char   * name,          /* connect parameters user:"str" */
         * lang,          /*                    lang:"C" */
         * version,       /*                    version:"1.1" */
         * user,          /*                    user:"str" */
         * pass,          /*                    pass:"str" */
         * auth_token;    /*                    auth_token:"str" */
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

struct NatsMsgTransform {
  md::MDMsgMem spc;
  const void * msg,
             * hdr;
  uint32_t     msg_len,
               hdr_len,
               msg_enc,
               idx_ref;
  NatsStr    & sid;
  bool         is_ready,
               is_converted;

  NatsMsgTransform( kv::EvPublish &pub,  NatsStr &id )
    : msg( pub.msg ), hdr( 0 ), msg_len( pub.msg_len ), hdr_len( pub.hdr_len ),
      msg_enc( pub.msg_enc ), idx_ref( 0 ), sid( id ), is_ready( false ),
      is_converted( false ) {
    if ( pub.hdr_len > 0 ) {
      this->hdr = this->msg;
      this->msg = &((const char *) this->msg)[ pub.hdr_len ];
      this->msg_len -= pub.hdr_len;
    }
  }

  void check_transform( bool bin ) {
    if ( bin || this->msg_len == 0 || this->msg_enc == md::MD_STRING )
      return;
    this->transform();
  }
  void transform( void ) noexcept;
};

enum {
  ADD_SUB           = 0,
  PUB_MSG           = 1,
  HPUB_MSG          = 2,
  RCV_MSG           = 3,
  HRCV_MSG          = 4,
  REM_SID           = 5,
  NEED_MORE         = 6,
  IS_CONNECT        = 7,
  IS_OK             = 8,
  IS_ERR            = 9,
  IS_PING           = 10,
  IS_PONG           = 11,
  SKIP_SPACE        = 12,
  IS_INFO           = 13,

  DO_ERR            = 32,
  DO_OK             = 64,
  FLOW_BACKPRESSURE = 128
};

struct NatsMsg {
  uint32_t   kw,            /* NATS_KW_... */
             subject_len,   /* size of subject */
             reply_len,     /* size of reply */
             sid_len,       /* size of sid */
             queue_len;     /* size of queue */
  size_t     size;          /* size of message */
  uint64_t   msg_len,       /* size of msg_ptr for publish */
             hdr_len,       /* size of hdrs */
             max_msgs;      /* unsub param */
  char     * line,          /* first line */
           * msg_ptr,       /* PUB <subject> [reply] <size>\r\n<msg> */
           * subject,       /* either pub or sub subject */
           * reply,         /* pub reply */
           * sid,           /* SUB <subject> [queue] <sid> */
           * queue;         /* queue of sub */

  NatsMsg() {
    ::memset( (void *) this, 0, sizeof( *this ) );
  }
  int parse_msg( char *start,  char *end ) noexcept;
};

enum NatsState { /* msg_state */
  NATS_HAS_TIMER    = 1, /* timer running */
  NATS_BACKPRESSURE = 2, /* backpressure */
  NATS_BUFFERSIZE   = 4  /* input size over recv highwater */
};

struct EvNatsService : public kv::EvConnection, public kv::BPData {
  void * operator new( size_t, void *ptr ) { return ptr; }
  kv::RoutePublish & sub_route;
  EvNatsListen     & listen;

  NatsSubMap map;
  uint16_t   nats_state,   /* ascii hdr mode or binary blob mode */
             prefix_len,
             session_len;
  NatsLogin  user;
  char       prefix[ 16 ],
             session[ MAX_SESSION_LEN ];
  uint64_t   timer_id;

  EvNatsService( kv::EvPoll &p,  const uint8_t t,  EvNatsListen &l )
    : kv::EvConnection( p, t ), sub_route( l.sub_route ), listen( l ) {}

  void initialize_state( const char *pre,  size_t prelen,  uint64_t id ) {
    this->nats_state  = 0;
    this->user.release();
    this->prefix_len  = prelen;
    ::memcpy( this->prefix, pre, prelen );
    this->session_len = 0;
    this->timer_id    = id;
    this->bp_flags    = kv::BP_NOTIFY;
  }
  void add_sub( NatsMsg &msg ) noexcept;
  void rem_sid( NatsMsg &msg ) noexcept;
  void rem_all_sub( void ) noexcept;
  enum { NATS_FLOW_GOOD = 0, NATS_FLOW_BACKPRESSURE = 1, NATS_FLOW_STALLED = 2 };
  int fwd_pub( NatsMsg &msg ) noexcept;
  bool fwd_msg( kv::EvPublish &pub,  NatsMsgTransform &xf ) noexcept;
  void parse_connect( const char *buf,  size_t sz ) noexcept;
  bool on_inbox_reply( kv::EvPublish &pub ) noexcept;
  /* EvSocket */
  virtual void process( void ) noexcept;
  virtual void process_close( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept;
  virtual void read( void ) noexcept;
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept;
  virtual uint8_t is_subscribed( const kv::NotifySub &sub ) noexcept;
  virtual uint8_t is_psubscribed( const kv::NotifyPattern &pat ) noexcept;

  virtual bool get_service( void *host,  uint16_t &svc ) noexcept;
  virtual bool set_session( const char session[ MAX_SESSION_LEN ] ) noexcept;
  virtual size_t get_userid( char userid[ MAX_USERID_LEN ] ) noexcept;
  virtual size_t get_session( uint16_t svc,
                              char session[ MAX_SESSION_LEN ] ) noexcept;
  virtual size_t get_subscriptions( uint16_t svc,
                                    kv::SubRouteDB &subs ) noexcept;
  virtual size_t get_patterns( uint16_t svc,  int pat_fmt,
                               kv::SubRouteDB &pats ) noexcept;
  virtual void on_write_ready( void ) noexcept;
};

/* presumes little endian, 0xdf masks out 0x20 for toupper() */
#define NATS_KW( c1, c2, c3, c4 ) ( ( (uint32_t) ( c4 & 0xdf ) << 24 ) | \
                                    ( (uint32_t) ( c3 & 0xdf ) << 16 ) | \
                                    ( (uint32_t) ( c2 & 0xdf ) << 8 ) | \
                                    ( (uint32_t) ( c1 & 0xdf ) ) )
#define NATS_KW_OK1     NATS_KW( '+', 'O', 'K', '\r' )
#define NATS_KW_OK2     NATS_KW( '+', 'O', 'K', '\n' )
#define NATS_KW_MSG1    NATS_KW( 'M', 'S', 'G', ' ' )
#define NATS_KW_MSG2    NATS_KW( 'M', 'S', 'G', '\t' )
#define NATS_KW_HMSG    NATS_KW( 'H', 'M', 'S', 'G' )
#define NATS_KW_ERR     NATS_KW( '-', 'E', 'R', 'R' )
#define NATS_KW_SUB1    NATS_KW( 'S', 'U', 'B', ' ' )
#define NATS_KW_SUB2    NATS_KW( 'S', 'U', 'B', '\t' )
#define NATS_KW_PUB1    NATS_KW( 'P', 'U', 'B', ' ' )
#define NATS_KW_PUB2    NATS_KW( 'P', 'U', 'B', '\t' )
#define NATS_KW_HPUB    NATS_KW( 'H', 'P', 'U', 'B' )
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
#define NATS_JS_HEADERS      NATS_KW( 'H', 'E', 'A', 'D' )
#define NATS_JS_NO_RESPOND   NATS_KW( 'N', 'O', '_', 'R' )
#define NATS_JS_BINARY       NATS_KW( 'B', 'I', 'N', 'A' )
#define NATS_JS_SERVER       NATS_KW( 'S', 'E', 'R', 'V' )
#define NATS_JS_MAX_PAYLOAD  NATS_KW( 'M', 'A', 'X', '_' )
#define NATS_JS_CONNECT_URLS NATS_KW( 'C', 'O', 'N', 'N' )
#define NATS_JS_CLIENT       NATS_KW( 'C', 'L', 'I', 'E' )
#define NATS_JS_HOST         NATS_KW( 'H', 'O', 'S', 'T' )
#define NATS_JS_PORT         NATS_KW( 'P', 'O', 'R', 'T' )
#define NATS_JS_GIT_COMMIT   NATS_KW( 'G', 'I', 'T', '_' )

struct NatsArgs {
  static const size_t MAX_NATS_ARGS = 4; /* <subject> <sid> [reply] */
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
  static char * parse_end_size( char *start,  char *end,  uint64_t &sz,
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
