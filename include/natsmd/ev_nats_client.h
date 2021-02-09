#ifndef __rai_natsmd__ev_nats_client_h__
#define __rai_natsmd__ev_nats_client_h__

#include <natsmd/ev_nats.h>

namespace rai {
namespace natsmd {

struct EvNatsClient : public kv::EvConnection, public kv::RouteNotify {
  void * operator new( size_t, void *ptr ) { return ptr; }
  char     * msg_ptr;     /* ptr to the msg blob */
  size_t     msg_len;     /* size of the current message blob */
  NatsState  msg_state;   /* ascii hdr mode or binary blob mode */
  char     * subject;     /* either pub or sub subject w/opt reply: */
  size_t     subject_len; /* MSG <subject> <sid> [reply-to] <#bytes> [pay] */
  char     * reply;       /* reply-to in MSG */
  size_t     reply_len;   /* len of reply */
  char     * sid;         /* <sid> of SUB */
  size_t     sid_len;     /* len of sid */
  char     * msg_len_ptr;    /* ptr to msg_len ascii */
  size_t     msg_len_digits; /* number of digits in msg_len */
  size_t     tmp_size;       /* amount of buffer[] free */
  char       buffer[ 1024 ]; /* ptrs below index into this space */

  int        protocol;    /* == 1 */
  const char * name,
             * lang,
             * version,
             * user,
             * pass,
             * auth_token;

  EvNatsClient( kv::EvPoll &p ) noexcept;

  static EvNatsClient * create_nats_client( kv::EvPoll &p ) noexcept;

  bool connect( const char *host,  int port ) noexcept;

  void initialize_state( void ) {
    this->msg_ptr   = NULL;
    this->msg_len   = 0;
    this->msg_state = NATS_HDR_STATE;
    this->subject = this->reply = this->sid = this->msg_len_ptr = NULL;
    this->subject_len = this->reply_len = this->sid_len =
    this->msg_len_digits = 0;
    this->tmp_size = sizeof( this->buffer );
  }
  bool fwd_pub( void ) noexcept;
  void parse_info( const char *info,  size_t infolen ) noexcept;
  void do_shutdown( void ) noexcept;

  virtual void on_connect( void ) noexcept; /* notifcation of ready states */
  virtual void on_shutdown( uint64_t bytes_lost ) noexcept;

  /* EvSocket */
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept;

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
