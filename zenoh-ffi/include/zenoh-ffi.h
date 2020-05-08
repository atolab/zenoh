#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>


typedef struct ZNSession ZNSession;

void zn_close(ZNSession *session);

unsigned long zn_declare_resource(ZNSession *session, const char *r_name);

unsigned long zn_declare_resource_ws(ZNSession *session, unsigned long rid, const char *suffix);

ZNSession *zn_open(const char *locator);
