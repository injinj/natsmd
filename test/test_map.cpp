#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <natsmd/nats_map.h>

using namespace rai;
using namespace kv;
using namespace natsmd;

static size_t
get_args( char *buf,  char **args,  size_t *arglen,  size_t maxargs ) noexcept
{
  char * end = &buf[ ::strlen( buf ) ];
  size_t argc = 0;
  for (;;) {
    while ( buf < end && *buf <= ' ' )
      buf++;
    if ( buf == end || argc == maxargs )
      break;
    args[ argc ] = buf;
    while ( buf < end && *buf > ' ' )
      buf++;
    arglen[ argc ] = buf - args[ argc ];
    argc++;
  }
  return argc;
}

int
main( void )
{
  NatsSubMap    map;
  NatsStr       subj, sid, msg, pre;
  char          buf[ 1024 ];
  char        * args[ 8 ];
  size_t        argc, arglen[ 8 ];
  NatsLookup    look;
  uint64_t      maxmsgs;
  NatsSubStatus status;
  bool          b, coll;

  for (;;) {
    if ( 0 ) {
    help:;
      printf( "sub <subj> <sid>\n"
              "unsub <sid> [maxmsgs]\n"
              "pub <subj> <msg>\n"
              "tab\nq\n" );
    }
    if ( fgets( buf, sizeof( buf ), stdin ) == NULL )
      break;
    if ( buf[ 0 ] == '#' || buf[ 0 ] == '\n' )
      continue;
    argc = get_args( buf, args, arglen, 3 );
    if ( argc == 1 ) {
      if ( buf[ 0 ] == 'q' )
        break;
      if ( buf[ 0 ] == 't' ) {
        map.print();
        continue;
      }
    }
    if ( argc == 0 )
      goto help;
    switch ( args[ 0 ][ 0 ] ) {
      case 's': /* sub <subj> <sid> */
        if ( argc != 3 )
          goto help;
        subj.set( args[ 1 ], arglen[ 1 ] );
        sid.set ( args[ 2 ], arglen[ 2 ] );
        if ( subj.is_wild() ) {
          PatternCvt cvt;
          if ( cvt.convert_rv( subj.str, subj.len ) != 0 )
            status = NATS_BAD_PATTERN;
          else {
            NatsStr pre( subj.str, cvt.prefixlen );
            status = map.put_wild( subj, cvt, pre, sid, coll );
          }
        }
        else {
          status = map.put( subj, sid, coll );
        }
        printf( "%s coll=%s\n", nats_status_str( status ), coll ? "t" : "f" );
        break;
      case 'u': /* unsub <sid> [maxmsgs] */
        if ( argc < 2 || argc > 3 )
          goto help;
        if ( argc == 3 )
          maxmsgs = strtol( args[ 2 ], NULL, 0 );
        else
          maxmsgs = 0;
        sid.set ( args[ 1 ], arglen[ 1 ] );
        status = map.unsub( sid, maxmsgs, look, coll );
        printf( "%s coll=%s\n", nats_status_str( status ), coll ? "t" : "f" );
        if ( status == NATS_EXPIRED ) {
          if ( look.rt != NULL )
            printf( "remove %.*s\n", look.rt->len, look.rt->value );
          else
            printf( "remove %.*s\n", look.match->len, look.match->value );
          map.unsub_remove( look );
        }
        break;
      case 'p': /* pub <subj> <msg> */
        if ( argc != 3 )
          goto help;
        subj.set( args[ 1 ], arglen[ 1 ] );
        msg.set( args[ 2 ], arglen[ 2 ] );
        status = map.lookup_publish( subj, look );
        if ( status != NATS_NOT_FOUND ) {
          for ( b = look.rt->first_sid( sid ); b;
                b = look.rt->next_sid( sid ) ) {
            printf( "pub %.*s -> %.*s\n", msg.len, msg.str, sid.len, sid.str );
          }
          if ( status == NATS_EXPIRED ) {
            status = map.expired( look, coll );
            printf( "%s coll=%s\n", nats_status_str( status ), coll ? "t" : "f" );
            if ( status == NATS_EXPIRED ) {
              printf( "remove %.*s\n", look.rt->len, look.rt->value );
              map.unsub_remove( look );
            }
          }
        }
        for ( uint16_t i = 0; i < 64; i++ ) {
          if ( i >= arglen[ 1 ] )
            break;
          pre.set( args[ 1 ], i );
          status = map.lookup_pattern( pre, subj, look );
          for (;;) {
            if ( status == NATS_NOT_FOUND )
              break;
            for ( b = look.match->first_sid( sid ); b;
                  b = look.match->next_sid( sid ) ) {
              printf( "pub %.*s -> %.*s (%.*s)\n",
                      msg.len, msg.str, sid.len, sid.str,
                      look.match->len, look.match->value );
            }
            if ( status == NATS_EXPIRED ) {
              status = map.expired_pattern( look, coll );
              printf( "%s coll=%s\n", nats_status_str( status ), coll ? "t" : "f" );
              if ( status == NATS_EXPIRED ) {
                printf( "remove %.*s\n", look.match->len, look.match->value );
                map.unsub_remove( look );
                break;
              }
            }
            status = map.lookup_next( subj, look );
          }
        }
        break;
      default:
        goto help;
    }
  }
  return 0;
}

