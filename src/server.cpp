#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <natsmd/ev_nats.h>
#include <raikv/mainloop.h>

using namespace rai;
using namespace natsmd;
using namespace kv;

struct Args : public MainLoopVars { /* argv[] parsed args */
  int nats_port;
  Args() : nats_port( 0 ) {}
};

struct Loop : public MainLoop<Args> {
  Loop( EvShm &m,  Args &args,  int num ) :
    MainLoop<Args>( m, args, num ) {}

  EvNatsListen * nats_sv;
  bool nats_init( void ) {
    return Listen<EvNatsListen>( 0, this->r.nats_port, this->nats_sv,
                                 this->r.tcp_opts ); }
  bool init( void ) {
    if ( this->thr_num == 0 )
      printf( "nats:                 %d\n", this->r.nats_port );
    int cnt = this->nats_init();
    if ( this->thr_num == 0 )
      fflush( stdout );
    return cnt > 0;
  }
};

template<>
bool
MainLoop<Args>::initialize( void ) noexcept
{
  return ((Loop *) this)->init();
}

int
main( int argc, const char *argv[] )
{
  EvShm shm;
  Args  r;

  r.add_desc( "  -c nats  = listen nats port      (8866)\n" );
  if ( ! r.parse_args( argc, argv ) )
    return 1;
  if ( shm.open( r.map_name, r.db_num ) != 0 )
    return 1;
  printf( "nats_version:         " kv_stringify( NATSMD_VER ) "\n" );
  shm.print();
  r.nats_port = r.parse_port( argc, argv, "-c", "8866" );
  Runner<Args, Loop> runner( r, shm );
  if ( r.thr_error == 0 )
    return 0;
  return 1;
}
