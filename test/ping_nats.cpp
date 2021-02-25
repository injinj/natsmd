#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <time.h>
#include <signal.h>
#include <hdr_histogram.h>
#include <raikv/ev_net.h>
#include <raikv/ev_tcp.h>

using namespace rai;
using namespace kv;

static const size_t PING_MSG_SIZE = 32;
struct PingMsg {
  uint64_t ping_src,
           time_sent,
           seqno_sent;
  char     pad[ PING_MSG_SIZE - sizeof( uint64_t ) * 3 ];
};

struct SockData : public EvConnection {
  uint64_t timeout_usecs;
  char tmp[ 256 ];
  size_t tmp_off;

  SockData( EvPoll &p ) :
    EvConnection( p, p.register_type( "sock_data" ) ),
    timeout_usecs( 1 ), tmp_off( 0 ) {}

  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual void process_close( void ) noexcept final;

  bool send_ping( bool is_pong, uint64_t src, uint64_t now,  uint64_t seqno ) {
    PingMsg msg;
    char buf[ 256 ];
    msg.ping_src   = src;
    msg.time_sent  = now;
    msg.seqno_sent = seqno;
    for ( size_t i = 0; i < sizeof( msg.pad ); i++ )
      msg.pad[ i ] = 'A' + i;
    if ( is_pong )
      memcpy( buf, "PUB PONG 32\r\n", 13 );
    else
      memcpy( buf, "PUB PING 32\r\n", 13 );
    memcpy( &buf[ 13 ], &msg, 32 );
    ::memcpy( &buf[ 13 + 32 ], "\r\n", 2 );

    return this->send( buf, 13 + 32 + 2 );
  }
  size_t eat( size_t amt ) {
    this->tmp_off -= amt;
    if ( this->tmp_off > 0 )
      ::memmove( this->tmp, &this->tmp[ amt ], this->tmp_off );
    return this->tmp_off;
  }
  bool recv_ping( uint64_t &src, uint64_t &stamp, uint64_t &seqno ) {
    PingMsg msg;
    size_t  len = sizeof( this->tmp ) - this->tmp_off;
    if ( len > 0 ) {
      if ( this->recv( &this->tmp[ this->tmp_off ], len ) ) {
        len += this->tmp_off;
        this->tmp_off = len;
      }
    }
    len = this->tmp_off;
    if ( len >= 15 + 32 + 2 && ::memcmp( "MSG ", this->tmp, 4 ) == 0 ) {
      ::memcpy( &msg, &this->tmp[ 15 ], 32 );
      src   = msg.ping_src;
      stamp = msg.time_sent;
      seqno = msg.seqno_sent;
      this->eat( 15 + 32 + 2 );
      return true;
    }
    if ( len >= 5 && ::memcmp( "+OK\r\n", this->tmp, 5 ) == 0 ) {
      len = this->eat( 5 );
    }
    if ( len >= 6 && ::memcmp( "PING\r\n", this->tmp, 6 ) == 0 ) {
      static char pong[] = "PONG\r\n";
      this->send( pong, sizeof( pong ) - 1 );
      len = this->eat( 6 );
    }
    return false;
  }
  bool send( void *data,  size_t size ) noexcept;
  bool recv( void *data,  size_t &size ) noexcept;
};

void
SockData::process( void ) noexcept
{
}

void
SockData::process_close( void ) noexcept
{
}

void
SockData::release( void ) noexcept
{
}

bool
SockData::send( void *data,  size_t size ) noexcept
{
  this->append( data, size );
  this->EvConnection::write();
  return true;
}

bool
SockData::recv( void *data,  size_t &size ) noexcept
{
  this->EvConnection::read();
  if ( this->len - this->off > 0 ) {
    size_t xlen = this->len - this->off;
    if ( xlen > size )
      xlen = size;
    ::memcpy( data, &this->EvConnection::recv[ this->off ], xlen );
    size = xlen;
    this->off += xlen;
    return true;
  }
  return false;
}

static const char *
get_arg( int argc, char *argv[], int b, const char *f, const char *def )
{
  for ( int i = 1; i < argc - b; i++ )
    if ( ::strcmp( f, argv[ i ] ) == 0 )
      return argv[ i + b ];
  return def; /* default value */
}

bool quit;

void
sigint_handler( int )
{
  quit = true;
}

uint64_t
current_time_nsecs( void )
{
  struct timespec ts;
  clock_gettime( CLOCK_MONOTONIC, &ts );
  return (uint64_t) ts.tv_sec * 1000000000 + ts.tv_nsec;
}

int
main( int argc, char **argv )
{
  const char * ne = get_arg( argc, argv, 1, "-n", "127.0.0.1" ),
             * po = get_arg( argc, argv, 1, "-p", "4222" ),
             * us = get_arg( argc, argv, 1, "-u", "chris" ),
             * ct = get_arg( argc, argv, 1, "-c", 0 ),
             * re = get_arg( argc, argv, 0, "-r", 0 ),
             /** bu = get_arg( argc, argv, 0, "-b", 0 ),*/
             * he = get_arg( argc, argv, 0, "-h", 0 );
  uint64_t count = 0, warm = 0;
  
  if ( he != NULL || ne == NULL ) {
    fprintf( stderr,
             "%s [-n network] [-p port] [-u user] [-r] [-c count]\n"
             "  -n network = network to ping\n"
             "  -p port    = port to ping\n"
             "  -u user    = user to connect\n"
             "  -r         = reflect pings\n"
             "  -b         = busy wait\n"
             "  -c count   = number of pings\n", argv[ 0 ] );
    return 1;
  }
  if ( ct != NULL ) {
    if ( atoll( ct ) <= 0 ) {
      fprintf( stderr, "count should be > 0\n" );
    }
    count = (uint64_t) atoll( ct );
    warm  = count;
    count = warm / 100;
    if ( count == 0 )
      count = 1;
  }

  EvPoll   poll;
  poll.init( 5, false );
  SockData data( poll );
  if ( EvTcpConnection::connect( data, ne, atoi( po ),
                                 DEFAULT_TCP_CONNECT_OPTS )!=0){
    fprintf( stderr, "create NATS socket failed\n" );
    return 1;
  }

  signal( SIGINT, sigint_handler );
  struct hdr_histogram * histogram = NULL;
  uint64_t ping_ival = 1000000000,
           last      = current_time_nsecs(),
           next      = last + ping_ival,
           seqno     = 0,
           my_id     = getpid(),
           src, stamp, num, delta, now;
  /*int      spin_cnt = 0;*/
  bool     reflect  = ( re != NULL )/*,
           busywait = ( bu != NULL )*/,
           connected = false;
  while ( ! quit && ! connected ) {
    char msg[ 1024 ];
    size_t msglen = sizeof( msg );
    if ( data.recv( msg, msglen ) ) {
      printf( "%.*s", (int) msglen, msg );
      if ( msg[ msglen - 1 ] == '\n' ) {
        if ( ! connected ) {
          static char conn[] = "CONNECT {"
                                 "\"user\":\"%s\","
                                 "\"name\":\"ping\","
                                 "\"verbose\":false,"
                                 "\"echo\":false"
                               "}\r\n";
          char buf[ 1024 ];
          size_t len = ::snprintf( buf, sizeof( buf ), conn, us );
          data.send( buf, len );
          connected = true;
        }
      }
    }
  }
  if ( ! connected )
    quit = true;
  if ( ! quit ) {
    static char sub_pong[] = "SUB PONG 1\r\n",
                sub_ping[] = "SUB PING 1\r\n";
    if ( ! reflect )
      data.send( sub_pong, sizeof( sub_pong ) - 1 );
    else
      data.send( sub_ping, sizeof( sub_ping ) - 1 );
  }

  if ( ! reflect )
    hdr_init( 1, 1000000, 3, &histogram );
  while ( ! quit ) {
    if ( data.recv_ping( src, stamp, num ) ) {
      /*spin_cnt = 0;*/
      if ( src == my_id ) {
        now  = current_time_nsecs();
        next = now; /* send the next immediately */
        hdr_record_value( histogram, now - stamp );
        if ( count > 0 && --count == 0 ) {
          if ( warm > 0 ) {
             hdr_reset( histogram );
             count = warm;
             warm  = 0;
          }
          else {
            quit = true;
          }
        }
      }
      else { /* not mine, reflect */
        data.send_ping( reflect, src, stamp, num );
      }
    }
    /* activly send pings every second until response */
    else if ( ! reflect ) {
      now = current_time_nsecs();
      if ( now >= next ) {
        if ( data.send_ping( reflect, my_id, now, seqno ) ) {
          seqno++;
          last = now;
          next = now + ping_ival;
          data.timeout_usecs = 0;
          /*spin_cnt = 0;*/
        }
      }
      else {
        delta = next - now;
        if ( data.timeout_usecs > delta / 1000 )
          data.timeout_usecs = delta / 1000;
      }
    }
  }
  /*data.close();*/
  if ( ! reflect )
    hdr_percentiles_print( histogram, stdout, 5, 1000.0, CLASSIC );
  return 0;
}
