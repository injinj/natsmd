#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <stdint.h>
#include <inttypes.h>
#ifndef _MSC_VER
#include <unistd.h>
#else
#include <raikv/win.h>
#endif
#include <raikv/ev_net.h>
#include <raikv/ev_tcp.h>
#include <raikv/util.h>

using namespace rai;
using namespace kv;

/* Self-contained replacement for HdrHistogram_c, the only user of which in
 * this whole repo was this one test/dev tool -- not worth an external
 * library dependency just for a ping utility's latency report. Stores raw
 * nanosecond samples and sorts them for exact percentiles at print time
 * (no bucketed/compressed representation like real HDR histograms use),
 * which is simple, exactly correct, and plenty fast for a single ping
 * run's sample count. */
struct LatencyHist {
  uint64_t * vals;
  size_t     count,
             cap;

  LatencyHist() : vals( 0 ), count( 0 ), cap( 0 ) {}

  void record( uint64_t v ) {
    if ( this->count == this->cap ) {
      this->cap  = this->cap ? this->cap * 2 : 1024;
      this->vals = (uint64_t *)
        ::realloc( this->vals, this->cap * sizeof( this->vals[ 0 ] ) );
    }
    this->vals[ this->count++ ] = v;
  }
  void reset( void ) { this->count = 0; }

  static int cmp_u64( const void *a,  const void *b ) {
    uint64_t x = *(const uint64_t *) a,
             y = *(const uint64_t *) b;
    return x < y ? -1 : ( x > y ? 1 : 0 );
  }
  /* scale divides raw values before printing (e.g. 1000.0 to show usecs
   * for recorded nanosecond deltas), matching hdr_percentiles_print()'s
   * value_scale parameter's role. */
  void print_percentiles( FILE *fp,  double scale ) {
    if ( this->count == 0 ) {
      ::fprintf( fp, "no samples recorded\n" );
      return;
    }
    ::qsort( this->vals, this->count, sizeof( this->vals[ 0 ] ), cmp_u64 );

    static const double pcts[] =
      { 50.0, 75.0, 90.0, 95.0, 99.0, 99.9, 99.99, 99.999, 100.0 };
    double sum = 0;
    for ( size_t i = 0; i < this->count; i++ )
      sum += (double) this->vals[ i ];
    double mean = sum / (double) this->count;
    double var  = 0;
    for ( size_t i = 0; i < this->count; i++ ) {
      double d = (double) this->vals[ i ] - mean;
      var += d * d;
    }
    double stddev = ( this->count > 1 ) ?
      ::sqrt( var / (double) ( this->count - 1 ) ) : 0.0;

    ::fprintf( fp, "%11s %12s %10s\n", "Percentile", "Value", "Count<=" );
    for ( size_t i = 0; i < sizeof( pcts ) / sizeof( pcts[ 0 ] ); i++ ) {
      size_t idx = (size_t)
        ( ( pcts[ i ] / 100.0 ) * (double) ( this->count - 1 ) + 0.5 );
      if ( idx >= this->count )
        idx = this->count - 1;
      ::fprintf( fp, "%10.3f%% %12.3f %10" PRIu64 "\n", pcts[ i ],
                 (double) this->vals[ idx ] / scale, (uint64_t) ( idx + 1 ) );
    }
    ::fprintf( fp, "#[Mean          = %12.3f]\n", mean / scale );
    ::fprintf( fp, "#[StdDeviation  = %12.3f]\n", stddev / scale );
    ::fprintf( fp, "#[Min           = %12.3f]\n",
               (double) this->vals[ 0 ] / scale );
    ::fprintf( fp, "#[Max           = %12.3f]\n",
               (double) this->vals[ this->count - 1 ] / scale );
    ::fprintf( fp, "#[Total count   = %10" PRIu64 "]\n",
               (uint64_t) this->count );

    ::free( this->vals );
    this->vals  = 0;
    this->cap   = 0;
    this->count = 0;
  }
};

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

#ifndef _MSC_VER
#define get_process_id getpid
#else
#define get_process_id GetProcessId
#endif

int
main( int argc, char **argv )
{
  SignalHandler sighndl;
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

  EvPoll poll;
  poll.init( 5, false );
  sighndl.install();
  SockData data( poll );
  if ( EvTcpConnection::connect( data, ne, atoi( po ),
                                 DEFAULT_TCP_CONNECT_OPTS )!=0){
    fprintf( stderr, "create NATS socket failed\n" );
    return 1;
  }

  LatencyHist histogram;
  uint64_t ping_ival = 1000000000,
           last      = current_monotonic_time_ns(),
           next      = last + ping_ival,
           seqno     = 0,
           my_id     = get_process_id(),
           src, stamp, num, delta, now;
  /*int      spin_cnt = 0;*/
  bool     reflect  = ( re != NULL )/*,
           busywait = ( bu != NULL )*/,
           connected = false;
  while ( ! sighndl.signaled && ! connected ) {
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
    poll.quit = 1;
  if ( ! poll.quit ) {
    static char sub_pong[] = "SUB PONG 1\r\n",
                sub_ping[] = "SUB PING 1\r\n";
    if ( ! reflect )
      data.send( sub_pong, sizeof( sub_pong ) - 1 );
    else
      data.send( sub_ping, sizeof( sub_ping ) - 1 );
  }

  while ( ! poll.quit ) {
    if ( data.recv_ping( src, stamp, num ) ) {
      /*spin_cnt = 0;*/
      if ( src == my_id ) {
        now  = current_monotonic_time_ns();
        next = now; /* send the next immediately */
        histogram.record( now - stamp );
        if ( count > 0 && --count == 0 ) {
          if ( warm > 0 ) {
             histogram.reset();
             count = warm;
             warm  = 0;
          }
          else {
            poll.quit = 1;
          }
        }
      }
      else { /* not mine, reflect */
        data.send_ping( reflect, src, stamp, num );
      }
    }
    /* activly send pings every second until response */
    else if ( ! reflect ) {
      now = current_monotonic_time_ns();
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
    if ( sighndl.signaled )
      poll.quit = 1;
  }
  /*data.close();*/
  if ( ! reflect )
    histogram.print_percentiles( stdout, 1000.0 );
  return 0;
}
