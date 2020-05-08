#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>


typedef struct ZNSession ZNSession;

typedef struct ZProperties ZProperties;

void zn_close(ZNSession *session);

unsigned long zn_declare_resource(ZNSession *session, const char *r_name);

unsigned long zn_declare_resource_ws(ZNSession *session, unsigned long rid, const char *suffix);

ZNSession *zn_open(const char *locator, const ZProperties *_ps);

ZProperties *zn_properties_add(ZProperties *rps, unsigned long id, const char *value);

void zn_properties_free(ZProperties *rps);

ZProperties *zn_properties_make(void);

int zn_write(ZNSession *session, const char *r_name, const char *payload, unsigned int len);
