#ifndef __rai_natsmd__nats_map_h__
#define __rai_natsmd__nats_map_h__

extern "C" {
  struct pcre2_real_code_8;
  struct pcre2_real_match_data_8;
}

#include <raikv/route_ht.h>
#include <raikv/dlinklist.h>
#include <raikv/pattern_cvt.h>

namespace rai {
namespace natsmd {

enum NatsSubStatus {
  NATS_OK          = 0,
  NATS_IS_NEW      = 1, /* new subject or pattern subscribed */
  NATS_EXPIRED     = 2, /* subject or pattern dropped or publish == maxmsgs */
  NATS_NOT_FOUND   = 3, /* sid not found or publish subject not found */
  NATS_EXISTS      = 4, /* sid already mapped to a subject */
  NATS_TOO_MANY    = 5, /* sid list size > 64K */
  NATS_BAD_PATTERN = 6  /* failed to create pattern subscription */
};

const char * nats_status_str( NatsSubStatus status );

/* a subject or sid string */
struct NatsStr {
  const char * str; /* the data */
  uint16_t     len; /* array len of str[] */
  uint32_t     h;   /* cached hash value */

  NatsStr( const char *s = NULL,  uint16_t l = 0,  uint32_t ha = 0 )
    : str( s ), len( l ), h( ha ) {}

  void set( const char *s = NULL,  uint16_t l = 0,  uint32_t ha = 0 ) {
    this->str = s; this->len = l; this->h = ha;
  }
  bool ref( const char *p,  const char *end ) {
    if ( &p[ 2 ] > end )
      return false;
    ::memcpy( &this->len, p, 2 );
    if ( &p[ 2 + this->len ] > end )
      return false;
    this->str = &p[ 2 ];
    this->h   = 0;
    return true;
  }
  uint32_t hash( void ) {
    if ( this->h == 0 )
      this->h = kv_crc_c( this->str, this->len, 0 );
    return this->h;
  }
  bool equals( const NatsStr &val ) const {
    return val.len == this->len && ::memcmp( val.str, this->str, val.len ) == 0;
  }
  bool is_wild( void ) const {
    if ( this->len == 0 )
      return false;
    /* last char or fist char * or > */
    if ( this->str[ this->len - 1 ] == '*' ||
         this->str[ this->len - 1 ] == '>' ) {
      if ( this->len == 1 ) /* is first char */
        return true;
      if ( this->str[ this->len - 2 ] == '.' ) /* is last char */
        return true;
    }
    /* look for *. */
    const char *p = (const char *) ::memmem( this->str, this->len, "*.", 2 );
    if ( p != NULL ) {
      if ( p == this->str || p[ -1 ] == '.' )
        return true;
    }
    return false;
  }
  bool is_valid( void ) {
    if ( this->len == 0 )
      return false;
    /* if first is . or last is . */
    if ( this->str[ 0 ] == '.' || this->str[ this->len - 1 ] == '.' )
      return false;
    /* if any empty segments */
    return ::memmem( this->str, this->len, "..", 2 ) == NULL;
  }
};

struct SidEntry {
  uint64_t  max_msgs,   /* if a max number of messages */
            msg_cnt;    /* count of msgs at start of subscribe */
  uint32_t  subj_hash,  /* hash of subject or pattern */
            pref_hash,  /* hash of pattern prefix */
            hash;       /* hash of the sid string */
  uint16_t  len;        /* len of sid */
  char      value[ 2 ]; /* the sid value */

  void init( uint32_t h,  uint32_t pre_h = 0 ) {
    this->max_msgs  = 0;
    this->msg_cnt   = 0;
    this->subj_hash = h;
    this->pref_hash = pre_h;
  }
  void print( void ) noexcept;
};

struct NatsSubData {
  uint64_t msg_cnt,    /* number of messages matched */
           max_msgs;   /* if a max is set on one of the sids */
  uint32_t hash;       /* hash of subject */
  uint16_t refcnt,     /* count of sid references */
           subj_len,   /* len of subject */
           sid_off,    /* len of sids */
           len;        /* length of subject + sids */
  char     value[ 2 ]; /* the subject string + sids */

  void init( uint16_t len ) {
    this->msg_cnt  = 0;
    this->max_msgs = 0;
    this->refcnt   = 0;
    this->subj_len = len;
    this->sid_off  = len;
  }
  bool add_sid( const NatsStr &sid ) {
    if ( this->sid_off + sid.len + 2 > this->len )
      return false;
    char *p = &this->value[ this->sid_off ];
    this->sid_off += sid.len + 2;
    ::memcpy( p, &sid.len, 2 );
    ::memcpy( &p[ 2 ], sid.str, sid.len );
    this->refcnt++;
    return true;
  }
  char *end( void ) { return &this->value[ this->sid_off ]; }
  bool first_sid( NatsStr &sid ) {
    return sid.ref( &this->value[ this->subj_len ], this->end() );
  }
  bool next_sid( NatsStr &sid ) {
    return sid.ref( &sid.str[ sid.len ], this->end() );
  }
  void remove_sid( NatsStr &sid ) {
    size_t mvlen = this->end() - &sid.str[ sid.len ], /* len after sid  */
           off   = &sid.str[ -2 ] - this->value;      /* offset to start sid */
    ::memmove( &this->value[ off ], &sid.str[ sid.len ], mvlen );
    this->sid_off -= sid.len + 2;
    this->refcnt--;
  }
  bool remove_and_next_sid( NatsStr &sid ) {
    this->remove_sid( sid );
    return sid.ref( &sid.str[ -2 ], this->end() );
  }
  bool equals( const void *s,  uint16_t l ) const {
    return this->subj_len == l && ::memcmp( s, this->value, l ) == 0;
  }
  bool equals( const NatsStr &str ) const {
    return this->equals( str.str, str.len );
  }
  NatsSubStatus unsub( SidEntry &entry, uint64_t max_msgs ) {
    if ( entry.subj_hash != this->hash )
      return NATS_NOT_FOUND;
    NatsStr sid( entry.value, entry.len );
    NatsStr xsid;
    for ( bool b = this->first_sid( xsid ); b; b = this->next_sid( xsid ) ) {
      if ( sid.equals( xsid ) ) {
        if ( max_msgs != 0 ) {
          entry.max_msgs = entry.msg_cnt + max_msgs;
          if ( entry.max_msgs > this->msg_cnt ) { /* max must be more than cnt*/
            if ( this->max_msgs == 0 || entry.max_msgs < this->max_msgs )
              this->max_msgs = entry.max_msgs; /* trigger unsub on publish */
            return NATS_OK;
          }
        }
        this->remove_sid( xsid );
        return NATS_EXPIRED; /* sid removed */
      }
    }
    return NATS_NOT_FOUND; /* sid not a member of subject */
  }
  void print_sids( void ) noexcept;
};
struct NatsSubRoute : public NatsSubData {
  static bool equals( const NatsSubRoute &r, const void *s, uint16_t l ) {
    return r.NatsSubData::equals( s, l );
  }
  void print( void ) noexcept;
};

template<class Match>
struct NatsWildData {
  Match                   * next,
                          * back;
  pcre2_real_code_8       * re;   /* pcre to match the publish subject */
  pcre2_real_match_data_8 * md;

  NatsWildData( pcre2_real_code_8 *r,  pcre2_real_match_data_8 *m )
    : next( 0 ), back( 0 ), re( r ), md( m ) {}
};

struct NatsWildMatch : public NatsWildData<NatsWildMatch>, public NatsSubData {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  NatsWildMatch( NatsStr &subj,  NatsStr &sid,
                 pcre2_real_code_8 *re,  pcre2_real_match_data_8 *md )
      : NatsWildData( re, md ) {
    this->NatsSubData::init( subj.len );
    this->hash = subj.hash();
    this->len  = subj.len + sid.len + 2;
    ::memcpy( this->value, subj.str, subj.len );
    this->NatsSubData::add_sid( sid );
  }
  ~NatsWildMatch();

  static NatsWildMatch *create( NatsStr &subj,  NatsStr &sid,
                                kv::PatternCvt &cvt ) noexcept;
  static NatsWildMatch *resize_sid( NatsWildMatch *m,  NatsStr &sid ) noexcept;
  bool match( NatsStr &subj ) noexcept;
  void print( void ) noexcept;
};

struct NatsPatternRoute {
  uint32_t                     hash,       /* hash of the pattern prefix */
                               count;      /* count of matches */
  kv::DLinkList<NatsWildMatch> list;       /* list of patterns with same pref */
  uint16_t                     len;        /* length of the pattern prefix */
  char                         value[ 2 ]; /* the pattern prefix */

  void init( void ) {
    this->count = 0;
    this->list.init();
  }
  void print( void ) noexcept;
};

struct NatsLookup {
  NatsSubRoute     * rt;
  NatsPatternRoute * pat;
  NatsWildMatch    * match,
                   * next;
  kv::RouteLoc       loc;
  uint32_t           hash;
  void init( uint32_t h = 0 ) {
    this->rt    = NULL;
    this->pat   = NULL;
    this->match = NULL;
    this->next  = NULL;
    this->hash  = h;
  }
};

struct NatsSubTab
    : public kv::RouteVec<NatsSubRoute, nullptr, NatsSubRoute::equals> {
  bool rem_collision( NatsSubRoute *rt ) {
    kv::RouteLoc   loc;
    NatsSubRoute * rt2;
    rt->refcnt = 0;
    if ( (rt2 = this->find_by_hash( rt->hash, loc )) != NULL ) {
      do {
        if ( rt2->refcnt != 0 )
          return true;
      } while ( (rt2 = this->find_next_by_hash( rt->hash, loc )) != NULL );
    }
    return false;
  }
  NatsSubStatus find3( uint32_t h,  const char *sub,  size_t len,
                       bool &collision ) {
    kv::RouteLoc loc;
    uint32_t     hcnt;
    NatsSubRoute * rt = this->find2( h, sub, len, loc, hcnt );
    if ( rt == NULL ) {
      collision = ( hcnt > 0 );
      return NATS_NOT_FOUND;
    }
    collision = ( hcnt > 1 );
    return NATS_OK;
  }
};

struct NatsPatternTab : public kv::RouteVec<NatsPatternRoute> {
  bool rem_collision( NatsPatternRoute *rt,  NatsWildMatch *m ) {
    kv::RouteLoc       loc;
    NatsPatternRoute * rt2;
    NatsWildMatch    * m2;
    m->refcnt = 0;
    if ( (rt2 = this->find_by_hash( rt->hash, loc )) != NULL ) {
      do {
        for ( m2 = rt2->list.tl; m2 != NULL; m2 = m2->back ) {
          if ( m2->refcnt != 0 )
            return true;
        }
      } while ( (rt2 = this->find_next_by_hash( rt->hash, loc )) != NULL );
    }
    return false;
  }
  NatsSubStatus find3( uint32_t h,  const char *sub,  size_t len,
                       NatsPatternRoute *&rt,  bool &collision ) {
    kv::RouteLoc loc;
    uint32_t hcnt;
    rt = this->find2( h, sub, len, loc, hcnt );
    if ( rt == NULL ) {
      collision = ( hcnt > 0 );
      return NATS_NOT_FOUND;
    }
    collision = ( hcnt > 1 );
    return NATS_OK;
  }
};

struct NatsSubMap {
  NatsSubTab             sub_tab;
  NatsPatternTab         pat_tab;
  kv::RouteVec<SidEntry> sid_tab;

  void print( void ) noexcept;
  /* add a subject and sid */
  NatsSubStatus put( NatsStr &subj,  NatsStr &sid,  bool &collision ) {
    kv::RouteLoc loc;
    SidEntry   * entry;
    uint32_t     hcnt;

    entry = this->sid_tab.upsert( sid.hash(), sid.str, sid.len, loc );
    if ( entry == NULL || ! loc.is_new )
      return NATS_EXISTS;
    entry->init( subj.hash() );
    NatsSubRoute *rt = this->sub_tab.upsert2( subj.hash(), subj.str, subj.len,
                                              loc, hcnt );
    if ( loc.is_new ) {
      rt->init( subj.len );
      collision = ( hcnt > 0 );
    }
    else {
      entry->msg_cnt = rt->msg_cnt;
      collision = ( hcnt > 1 );
    }
    if ( ! rt->add_sid( sid ) ) {
      size_t newlen = (size_t) rt->sid_off + (size_t) sid.len + 2;
      if ( newlen > 0xffffU )
        rt = NULL;
      else
        rt = this->sub_tab.resize( subj.hash(), subj.str, (size_t) subj.len,
                                   newlen, loc );
      if ( rt == NULL ) {
        if ( loc.is_new )
          this->sub_tab.remove( loc );
        return NATS_TOO_MANY;
      }
      rt->add_sid( sid );
    }
    return loc.is_new ? NATS_IS_NEW : NATS_OK;
  }
  /* add a pattern and sid */
  NatsSubStatus put_wild( NatsStr &subj,  kv::PatternCvt &cvt,
                          NatsStr &pre,  NatsStr &sid,  bool &collision ) {
    kv::RouteLoc    loc;
    SidEntry      * entry;
    NatsWildMatch * m = NULL, * m2;
    uint32_t        hcnt;

    entry = this->sid_tab.upsert( sid.hash(), sid.str, sid.len, loc );
    if ( entry == NULL || ! loc.is_new )
      return NATS_EXISTS;
    entry->init( subj.hash(), pre.hash() );

    NatsPatternRoute *rt = this->pat_tab.upsert2( pre.hash(), pre.str,
                                                  pre.len, loc, hcnt );
    if ( loc.is_new ) {
      rt->init();
      collision = ( hcnt > 0 );
    }
    else {
      for ( m = rt->list.hd; m != NULL; m = m->next ) {
        if ( m->equals( subj ) )
          break;
      }
      collision = ( hcnt > 1 || m == NULL || rt->count > 1 );
    }
    /* new wildcard match */
    if ( m == NULL ) {
      m = NatsWildMatch::create( subj, sid, cvt );
      if ( m == NULL ) {
        if ( loc.is_new )
          this->pat_tab.remove( loc );
        return NATS_BAD_PATTERN;
      }
      rt->list.push_hd( m );
      rt->count++;
      return NATS_IS_NEW;
    }
    /* existing wildcard match */
    if ( ! m->add_sid( sid ) ) {
      rt->list.pop( m );
      m2 = NatsWildMatch::resize_sid( m, sid );
      if ( m2 == NULL ) {
        rt->list.push_hd( m );
        return NATS_TOO_MANY;
      }
      m = m2;
      rt->list.push_hd( m );
    }
    entry->msg_cnt = m->msg_cnt;
    return NATS_OK;
  }
  /* unsubscribe an sid */
  NatsSubStatus unsub( NatsStr &sid,  uint64_t max_msgs,  NatsLookup &look,
                       bool &collision ) {
    kv::RouteLoc   sid_loc;
    SidEntry     * entry;
    NatsSubStatus  status;
    uint32_t       count = 0;

    look.init();
    collision = false;
    entry = this->sid_tab.find( sid.hash(), sid.str, sid.len, sid_loc );
    if ( entry == NULL )
      return NATS_NOT_FOUND;
    if ( entry->pref_hash == 0 ) {
      look.hash = entry->subj_hash;
      look.rt = this->sub_tab.find_by_hash( look.hash, look.loc );
      if ( look.rt == NULL )
        return NATS_NOT_FOUND;
      for (;;) {
        status = look.rt->unsub( *entry, max_msgs );
        if ( status != NATS_NOT_FOUND ) {
          if ( status == NATS_EXPIRED ) {
            this->sid_tab.remove( sid_loc );
            if ( look.rt->refcnt == 0 ) {
              if ( count > 0 )
                collision = true;
              else {
                kv::RouteLoc next = look.loc;
                if ( this->sub_tab.find_next_by_hash( look.hash,
                                                      next ) != NULL )
                  collision = true;
              }
              return NATS_EXPIRED;
            }
          }
          return NATS_OK;
        }
        look.rt = this->sub_tab.find_next_by_hash( look.hash, look.loc );
        if ( look.rt == NULL )
          return NATS_NOT_FOUND;
        count++;
      }
    }
    else {
      look.hash = entry->pref_hash;
      look.pat = this->pat_tab.find_by_hash( look.hash, look.loc );
      if ( look.pat == NULL )
        return NATS_NOT_FOUND;
      for (;;) {
        for ( NatsWildMatch *m = look.pat->list.hd; m != NULL; m = m->next ) {
          if ( m->hash == entry->subj_hash ) {
            status = m->unsub( *entry, max_msgs );
            if ( status != NATS_NOT_FOUND ) {
              if ( status == NATS_EXPIRED ) {
                this->sid_tab.remove( sid_loc );
                if ( m->refcnt == 0 ) {
                  if ( count > 0 || look.pat->count > 1 )
                    collision = true;
                  else {
                    kv::RouteLoc next = look.loc;
                    if ( this->pat_tab.find_next_by_hash( look.hash,
                                                          next ) != NULL )
                      collision = true;
                  }
                  look.match = m;
                  return NATS_EXPIRED;
                }
              }
              return NATS_OK;
            }
          }
        }
        look.pat = this->pat_tab.find_next_by_hash( look.hash, look.loc );
        if ( look.pat == NULL )
          return NATS_NOT_FOUND;
        count++;
      }
    }
  }
  /* remove after unsubscribe */
  void unsub_remove( NatsLookup &look ) {
    if ( look.rt != NULL ) {
      this->sub_tab.remove( look.loc );
      look.rt = NULL;
    }
    else {
      look.pat->list.pop( look.match );
      delete look.match;
      look.match = NULL;
      if ( --look.pat->count == 0 ) {
        this->pat_tab.remove( look.loc );
        look.pat = NULL;
      }
    }
  }
  /* remove sids that are == to max_msgs */
  bool expire_sids( NatsSubData &data ) {
    kv::RouteLoc sid_loc;
    NatsStr      sid;
    uint64_t     new_max_msgs = 0;
    for ( bool b = data.first_sid( sid ); b; ) {
      SidEntry *entry = this->sid_tab.find( sid.hash(), sid.str, sid.len,
                                            sid_loc );
      if ( entry != NULL ) {
        /* if this entry is expired */
        if ( entry->max_msgs == data.max_msgs )
          this->sid_tab.remove( sid_loc );
        else { /* find the new max_msgs */
          if ( entry->max_msgs > data.max_msgs ) {
            if ( new_max_msgs == 0 || new_max_msgs > entry->max_msgs )
              new_max_msgs = entry->max_msgs; /* minimum > rt->max_msgs */
          }
          b = data.next_sid( sid );
          continue;
        }
      }
      b = data.remove_and_next_sid( sid );
    }
    data.max_msgs = new_max_msgs;
    return data.refcnt == 0; /* true if no more sids */
  }
  /* when sid expired after publish */
  NatsSubStatus expired( NatsLookup &look,  bool &collision ) {
    collision = false;
    if ( this->expire_sids( *look.rt ) ) {
      kv::RouteLoc loc;
      /* one of these is the current subject */
      if ( this->sub_tab.find_by_hash( look.hash, loc ) != NULL &&
           this->sub_tab.find_next_by_hash( look.hash, loc ) != NULL )
        collision = true;
      return NATS_EXPIRED;
    }
    return NATS_OK;
  }
  NatsSubStatus expired_pattern( NatsLookup &look,  bool &collision ) {
    collision = false;
    if ( this->expire_sids( *look.match ) ) {
      kv::RouteLoc loc;
      /* if multiple patterns with the same prefix */
      if ( look.pat->count > 1 )
        collision = true;
      /* one of these is the current subject */
      else if ( this->pat_tab.find_by_hash( look.hash, loc ) != NULL &&
                this->pat_tab.find_next_by_hash( look.hash, loc ) != NULL )
        collision = true;
      return NATS_EXPIRED;
    }
    return NATS_OK;
  }
  /* find the subject and sid list to publish */
  NatsSubStatus lookup_publish( NatsStr &subj,  NatsLookup &look ) {
    look.init( subj.hash() );
    look.rt = this->sub_tab.find( look.hash, subj.str, subj.len, look.loc );
    if ( look.rt == NULL )
      return NATS_NOT_FOUND;
    if ( ++look.rt->msg_cnt == look.rt->max_msgs )
      return NATS_EXPIRED;
    return NATS_OK;
  }
  /* find the pattern prefix and match pattern to publish */
  NatsSubStatus lookup_pattern( NatsStr &pre,  NatsStr &subj,
                                NatsLookup &look ) {
    look.init( pre.hash() );
    look.pat = this->pat_tab.find( look.hash, pre.str, pre.len, look.loc );
    if ( look.pat == NULL )
      return NATS_NOT_FOUND;
    for ( NatsWildMatch *m = look.pat->list.hd; m != NULL; m = m->next ) {
      if ( m->re == NULL || m->match( subj ) ) {
        look.match = m;
        look.next  = m->next;
        if ( ++m->msg_cnt == m->max_msgs )
          return NATS_EXPIRED;
        return NATS_OK;
      }
    }
    return NATS_NOT_FOUND;
  }
  /* find the next patter to publish after above, may match multiple */
  NatsSubStatus lookup_next( NatsStr &subj,  NatsLookup &look ) {
    for ( NatsWildMatch *m = look.next; m != NULL; m = m->next ) {
      if ( m->re == NULL || m->match( subj ) ) {
        look.match = m;
        look.next  = m->next;
        if ( ++m->msg_cnt == m->max_msgs )
          return NATS_EXPIRED;
        return NATS_OK;
      }
    }
    return NATS_NOT_FOUND;
  }
  void release( void ) {
    kv::RouteLoc       loc;
    NatsPatternRoute * rt;
    NatsWildMatch    * m,
                     * next;
    if ( (rt = this->pat_tab.first( loc )) != NULL ) {
      do {
        for ( m = rt->list.hd; m != NULL; m = next ) {
          next = m->next;
          delete m;
        }
      } while ( (rt = this->pat_tab.next( loc )) != NULL );
    }
    this->sid_tab.release();
    this->sub_tab.release();
    this->pat_tab.release();
  }
};

}
}
#endif
