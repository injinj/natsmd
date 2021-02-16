#ifndef __rai_natsmd__ev_nats_client_h__
#define __rai_natsmd__ev_nats_client_h__

#include <natsmd/ev_nats.h>
#include <raikv/dlinklist.h>

namespace rai {
namespace natsmd {

static const uint32_t TRAILER_MARK = 0xff44aa99U;
struct NatsTrailer { /* fragment trailer */
  uint64_t src_id,
           src_time;
  uint32_t off,
           msg_len,
           hash,
           mark;
  NatsTrailer( uint64_t src,  uint64_t t,  uint32_t h,  uint32_t len )
    : src_id( src ), src_time( t ), off( 0 ), msg_len( len ),
      hash( h ), mark( TRAILER_MARK ) {}
  NatsTrailer( void *msg,  size_t mlen ) : mark( 0 ) {
    if ( mlen > sizeof( *this ) )
      ::memcpy( this, &((char *) msg)[ mlen - sizeof( *this ) ],
                sizeof( *this ) );
  }
  bool is_fragment( uint32_t h,  size_t max_payload ) const {
    return this->mark == TRAILER_MARK && h == this->hash &&
           this->off < this->msg_len && this->msg_len > max_payload;
  }
};

struct NatsFragment {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  NatsFragment * next,
               * back;
  uint64_t       src_id,
                 src_time;
  uint32_t       hash,
                 off,
                 msg_len,
                 pad;

  NatsFragment( uint64_t src,  uint64_t t,  uint32_t h, uint32_t len )
    : next( 0 ), back( 0 ), src_id( src ), src_time( t ), hash( h ),
      off( 0 ), msg_len( len ), pad( 0 ) {}
  void *msg_ptr( void ) {
    return &this[ 1 ];
  }
};

struct EvNatsClientNotify {
  virtual void on_connect( void ) noexcept; /* notifcation of ready states */
  virtual void on_shutdown( uint64_t bytes_lost ) noexcept;
};

struct EvNatsClient : public kv::EvConnection, public kv::RouteNotify {
  void * operator new( size_t, void *ptr ) { return ptr; }
  char       * msg_ptr;     /* ptr to the msg blob */
  size_t       msg_len;     /* size of the current message blob */
  NatsState    msg_state;   /* ascii hdr mode or binary blob mode */
  char       * subject;     /* either pub or sub subject w/opt reply: */
  size_t       subject_len; /* MSG <subject> <sid> [reply-to] <#bytes> [pay] */
  char       * reply;       /* reply-to in MSG */
  size_t       reply_len;   /* len of reply */
  char       * sid;         /* <sid> of SUB */
  size_t       sid_len;     /* len of sid */
  char       * msg_len_ptr;    /* ptr to msg_len ascii */
  size_t       msg_len_digits; /* number of digits in msg_len */
  size_t       tmp_size;       /* amount of buffer[] free */
  char         buffer[ 1024 ]; /* ptrs below index into this space */
  uint32_t     next_sid;       /* first is 1 max is 1 << 30 */
  uint8_t      protocol,       /* from INFO */
               fwd_all_msgs,   /* send publishes */
               fwd_all_subs;   /* send subscriptons */
  size_t       max_payload;    /* 1024 * 1024 */
  const char * name,           /* CONNECT parm:  name="service" */
             * lang,                          /* lang="C" */
             * version,                       /* version="C" */
             * user,                          /* user="network" */
             * pass,                          /* pass="xxx" */
             * auth_token;                    /* auth_token="xxx" */
  kv::DLinkList<NatsFragment> frags_pending;
  kv::UIntHashTab    * sid_ht;
  kv::ULongHashTab   * sid_collision_ht;
  EvNatsClientNotify * notify;

  EvNatsClient( kv::EvPoll &p ) noexcept;

  static EvNatsClient * create_nats_client( kv::EvPoll &p ) noexcept;

  bool connect( const char *host,  int port,
                EvNatsClientNotify *n = NULL ) noexcept;
  void initialize_state( void ) {
    this->msg_ptr   = NULL;
    this->msg_len   = 0;
    this->msg_state = NATS_HDR_STATE;
    this->subject = this->reply = this->sid = this->msg_len_ptr = NULL;
    this->subject_len = this->reply_len = this->sid_len =
    this->msg_len_digits = 0;
    this->tmp_size = sizeof( this->buffer );
    this->next_sid = 1;
  }
  bool fwd_pub( void ) noexcept;
  void parse_info( const char *info,  size_t infolen ) noexcept;
  void do_shutdown( void ) noexcept;
  NatsFragment *merge_fragment( NatsTrailer &trail,  const void *msg,
                                size_t msg_len ) noexcept;
  void release_fragments( void ) noexcept;

  /* EvSocket */
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept;

  static const uint32_t SID_COLLISION = 1U << 31;
  uint32_t create_sid( uint32_t h,  const char *sub,  size_t sublen ) noexcept;
  uint32_t remove_sid( uint32_t h,  const char *sub,  size_t sublen ) noexcept;

  /* RouteNotify */
  virtual void on_sub( uint32_t h,  const char *sub,  size_t sublen,
                       uint32_t src_fd,  uint32_t rcnt,  char src_type,
                       const char *rep,  size_t rlen ) noexcept;
  virtual void on_unsub( uint32_t h,  const char *sub,  size_t sublen,
                         uint32_t src_fd,  uint32_t rcnt,
                         char src_type ) noexcept;
  virtual void on_psub( uint32_t h,  const char *pattern,  size_t patlen,
                        const char *prefix,  uint8_t prefix_len,
                        uint32_t src_fd,  uint32_t rcnt,
                        char src_type ) noexcept;
  virtual void on_punsub( uint32_t h,  const char *pattern,  size_t patlen,
                          const char *prefix,  uint8_t prefix_len,
                          uint32_t src_fd,  uint32_t rcnt,
                          char src_type ) noexcept;
};

}
}
#endif
