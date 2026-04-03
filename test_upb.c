#include "upb_out/upb.h"
#include <stdio.h>

int main() {
  upb_Arena *arena = upb_Arena_New();
  if (arena) {
    printf("Successfully created upb arena!\n");
    upb_Arena_Free(arena);
    return 0;
  }
  printf("Failed to create upb arena.\n");
  return 1;
}
