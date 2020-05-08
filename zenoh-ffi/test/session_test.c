#include "zenoh-ffi.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

int main(int argc, char** argv) {
  ZNSession *s = zn_open("");
  if (s == 0) {
    printf("Error creating session!\n");
    exit(-1);
  } 
  sleep(3);
  zn_close(s);  
}