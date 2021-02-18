#ifndef __rai_natsmd__ev_nats_client_h__
#define __rai_natsmd__ev_nats_client_h__

#include <natsmd/ev_nats.h>
#include <raikv/dlinklist.h>

namespace rai {
namespace natsmd {

/* a fragment is marked at the end of a publish with these fields */
static const uint32_t TRAILER_MARK = 0xff44aa99U; /* spells frag, hah */
struct NatsTrailer { /* trailer bytes */
  uint64_t src_id,   /* source of the fragment */
           src_time; /* time that the mssage was published */
  uint32_t off,      /* offset of the fragment */
           msg_len,  /* total size of the message */
           hash,     /* hash of the subject */
           mark;     /* TRAILER_MARK */
  /* create new trailer on publish */
  NatsTrailer( uint64_t src,  uint64_t t,  uint32_t h,  uint32_t len )
    : src_id( src ), src_time( t ), off( 0 ), msg_len( len ),
      hash( h ), mark( TRAILER_MARK ) {}
  /* create from message buffer on recv */
  NatsTrailer( void *msg,  size_t mlen ) : mark( 0 ) {
    if ( mlen > sizeof( *this ) )
      ::memcpy( this, &((char *) msg)[ mlen - sizeof( *this ) ],
                sizeof( *this ) );
  }
  /* these must match to be considered for de-fragmenting */
  bool is_fragment( uint32_t h,  size_t max_payload ) const {
    return this->mark == TRAILER_MARK && h == this->hash &&
           this->off < this->msg_len && this->msg_len > max_payload;
  }
};

/* messages larger than max_payload are fragmented */
struct NatsFragment {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  NatsFragment * next,     /* list links of message fragments pending */
               * back;
  uint64_t       src_id,   /* the source that created the frags */
                 src_time; /* the time that the message was published */
  uint32_t       hash,     /* hash of the subject envelope */
                 off,      /* offset of the fragments recvd */
                 msg_len,  /* total length of fragment */
                 pad;      /* alignment */
  /* data follows */
  NatsFragment( uint64_t src,  uint64_t t,  uint32_t h, uint32_t len )
    : next( 0 ), back( 0 ), src_id( src ), src_time( t ), hash( h ),
      off( 0 ), msg_len( len ), pad( 0 ) {}
  void *msg_ptr( void ) {
    return &this[ 1 ];
  }
};

/* notify when connected after NATS INFO message is processed */
struct EvNatsClientNotify {
  virtual void on_connect( void ) noexcept; /* notifcation of ready states */
  virtual void on_shutdown( uint64_t bytes_lost,  const char *err,
                            size_t errlen ) noexcept;
};

/* wildcards subscribed by prefix */
struct NatsPrefix {
  uint32_t hash;        /* hash of wildcard prefix */
  uint32_t sid;         /* sid forwarded to NATS is -sid */
  uint16_t len;         /* length of prefix */
  char     value[ 2 ];  /* the prefix */
  bool equals( const void *s,  uint16_t l ) const {
    return l == this->len && ::memcmp( s, this->value, l ) == 0;
  }
  void copy( const void *s,  uint16_t l ) {
    ::memcpy( this->value, s, l );
  }
};

/* a connection to a NATS server */
struct EvNatsClient : public kv::EvConnection, public kv::RouteNotify {
  void * operator new( size_t, void *ptr ) { return ptr; }
  char       * msg_ptr;        /* ptr to the msg blob */
  size_t       msg_len;        /* size of the current message blob */
  NatsState    msg_state;      /* ascii hdr mode or binary blob mode */
  char       * subject;        /* either pub or sub subject w/opt reply: */
  size_t       subject_len;    /* MSG <subject> <sid> [reply] <#bytes> [pay] */
  char       * reply;          /* [reply] in MSG */
  size_t       reply_len;      /* len of reply */
  char       * sid;            /* <sid> of SUB */
  size_t       sid_len;        /* len of sid */
  char       * msg_len_ptr;    /* ptr to msg_len ascii */
  size_t       msg_len_digits, /* number of digits in msg_len */
               tmp_size;       /* amount of buffer[] free */
  char         buffer[ 1024 ], /* ptrs below index into this space */
             * err;            /* if -ERR, then err points to end of buffer */
  size_t       err_len;        /* length of err */
  uint32_t     next_sid;       /* first is 1 max is 1 << 30 */
  uint8_t      protocol,       /* from INFO */
               fwd_all_msgs,   /* send publishes */
               fwd_all_subs;   /* send subscriptons */
  uint32_t     wild_prefix_char[ 3 ]; /* first char of wildcard [ '!' -> 127 ] */
  size_t       max_payload;    /* 1024 * 1024 */
  const char * name,           /* CONNECT parm:  name="service" */
             * lang,                          /* lang="C" */
             * version,                       /* version="1.0" */
             * user,                          /* user="network" */
             * pass,                          /* pass="xxx" */
             * auth_token;                    /* auth_token="xxx" */
  kv::DLinkList<NatsFragment> frags_pending; /* large message fragments */
  kv::UIntHashTab        * sid_ht;           /* hash32(subject) -> sid */
  kv::ULongHashTab       * sid_collision_ht; /* hash64 -> sid hash32 collision*/
  EvNatsClientNotify     * notify;           /* notify on_connect, on_shutdown */
  kv::RouteVec<NatsPrefix> pat_tab;          /* wildcard patterns */
  uint32_t wild_prefix_char_cnt[ 96 ];       /* count of all wildcard[ 0 ] */

  EvNatsClient( kv::EvPoll &p ) noexcept;
  static EvNatsClient * create_nats_client( kv::EvPoll &p ) noexcept;
  /* connect to a NATS server */
  bool connect( const char *host,  int port,
                EvNatsClientNotify *n = NULL ) noexcept;
  bool is_connected( void ) const {
    return this->EvSocket::fd != -1;
  }
  /* restart the protocol parser */
  void initialize_state( void ) {
    this->msg_ptr        = NULL;
    this->msg_len        = 0;
    this->msg_state      = NATS_HDR_STATE;
    this->subject        = this->reply = this->sid = this->msg_len_ptr = NULL;
    this->subject_len    = this->reply_len = this->sid_len =
    this->msg_len_digits = 0;
    this->tmp_size       = sizeof( this->buffer );
    this->err            = NULL;
    this->err_len        = 0;
    this->next_sid       = 1;
    for ( int i = 0; i < 3; i++ )
      this->wild_prefix_char[ i ] = 0;
    for ( int j = 0; j < 96; j++ )
      this->wild_prefix_char_cnt[ j ] = 0;
  }
  /* track which subjects are wildcards by first char */
  void set_wildcard_match( uint8_t c ) { /* 0 = all subjects */
    uint8_t bit = ( c > ' ' ? ( ( c - ' ' ) & 0x5fU ) : 0 );
    this->wild_prefix_char[ bit >> 5 ] |= 1U << ( bit & 31 );
    this->wild_prefix_char_cnt[ bit ]++;
  }
  /* when a wildcard unsubscribed */
  void clear_wildcard_match( uint8_t c ) {
    uint8_t bit = ( c > ' ' ? ( ( c - ' ' ) & 0x5fU ) : 0 );
    if ( --this->wild_prefix_char_cnt[ bit ] == 0 )
      this->wild_prefix_char[ bit >> 5 ] &= ~( 1U << ( bit & 31 ) );
  }
  /* how many possible wildcard matches for a subject */
  uint32_t possible_matches( uint8_t c ) const {
    uint8_t bit = ( c - ' ' ) & 0x5fU;
    return this->wild_prefix_char_cnt[ 0 ] +
           this->wild_prefix_char_cnt[ bit ];
  }
  /* if a subject may have a wildcard match */
  bool test_wildcard_match( uint8_t c ) const {
    if ( ( this->wild_prefix_char[ 0 ] & 1 ) != 0 ) /* all subjects */
      return true;
    uint8_t bit = ( c - ' ' ) & 0x5fU;
    return ( this->wild_prefix_char[ bit >> 5 ] & ( 1U << ( bit & 31 ) ) ) != 0;
  }
  /* forward NATS MSG to subscribers */
  bool fwd_pub( void ) noexcept;
  /* only forward one sid per message, if overlapping wildcards are subscribed */
  bool deduplicate_wildcard( kv::EvPublish &pub ) noexcept;
  /* parse the NATS INFO message */
  void parse_info( const char *info,  size_t infolen ) noexcept;
  /* when no subscribers left, NATS connection can be shutdown */
  void do_shutdown( void ) noexcept;
  /* merge a NATS MSG into a larger than max_payload message*/
  NatsFragment *merge_fragment( NatsTrailer &trail,  const void *msg,
                                size_t msg_len ) noexcept;
  /* release all frags on shutdown */
  void release_fragments( void ) noexcept;
  /* save error strings that occur while processing */
  void save_error( const char *buf,  size_t len ) noexcept;

  /* EvSocket */
  virtual void process( void ) noexcept final; /* decode read buffer */
  virtual void release( void ) noexcept final; /* after shutdown release mem */
  virtual bool on_msg( kv::EvPublish &pub ) noexcept; /* fwd to NATS network */

  static const uint32_t SID_COLLISION = 1U << 31; /* 2 subjects have hash coll */
  /* track the sid of subjects for UNSUB */
  uint32_t create_sid( uint32_t h,  const char *sub,  size_t sublen ) noexcept;
  /* find the subject sid and remove it */
  uint32_t remove_sid( uint32_t h,  const char *sub,  size_t sublen ) noexcept;
  /* RouteNotify */
  /* a new subscription */
  virtual void on_sub( uint32_t h,  const char *sub,  size_t sublen,
                       uint32_t src_fd,  uint32_t rcnt,  char src_type,
                       const char *rep,  size_t rlen ) noexcept;
  /* an unsubscribed sub */
  virtual void on_unsub( uint32_t h,  const char *sub,  size_t sublen,
                         uint32_t src_fd,  uint32_t rcnt,
                         char src_type ) noexcept;
  /* a new pattern subscription */
  virtual void on_psub( uint32_t h,  const char *pattern,  size_t patlen,
                        const char *prefix,  uint8_t prefix_len,
                        uint32_t src_fd,  uint32_t rcnt,
                        char src_type ) noexcept;
  /* an unsubscribed pattern sub */
  virtual void on_punsub( uint32_t h,  const char *pattern,  size_t patlen,
                          const char *prefix,  uint8_t prefix_len,
                          uint32_t src_fd,  uint32_t rcnt,
                          char src_type ) noexcept;
};

}
}
#endif
