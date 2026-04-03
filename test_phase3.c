#include "upb_out/upb.h"
#include <stdio.h>
#include <stdlib.h>

int main() {
    // 1. Read person.pb
    FILE* f = fopen("person.pb", "rb");
    if (!f) {
        printf("Failed to open person.pb\n");
        return 1;
    }
    
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);
    
    char* buf = malloc(size);
    if (!buf) {
        printf("Failed to allocate memory\n");
        fclose(f);
        return 1;
    }
    
    if (fread(buf, 1, size, f) != size) {
        printf("Failed to read file\n");
        free(buf);
        fclose(f);
        return 1;
    }
    fclose(f);
    
    // 2. Initialize upb
    upb_Arena* arena = upb_Arena_New();
    upb_DefPool* pool = upb_DefPool_New();
    
    // 3. Parse FileDescriptorSet
    google_protobuf_FileDescriptorSet* set = google_protobuf_FileDescriptorSet_parse(buf, size, arena);
    if (!set) {
        printf("Failed to parse FileDescriptorSet\n");
        free(buf);
        upb_DefPool_Free(pool);
        upb_Arena_Free(arena);
        return 1;
    }
    free(buf);
    
    // 4. Get FileDescriptorProto from set
    size_t file_count;
    const google_protobuf_FileDescriptorProto* const* files = google_protobuf_FileDescriptorSet_file(set, &file_count);
    if (file_count == 0) {
        printf("No files in FileDescriptorSet\n");
        upb_DefPool_Free(pool);
        upb_Arena_Free(arena);
        return 1;
    }
    
    // 5. Add to pool
    upb_Status status;
    upb_Status_Clear(&status);
    const upb_FileDef* file = upb_DefPool_AddFile(pool, files[0], &status);
    if (!file) {
        printf("Failed to add file to pool: %s\n", upb_Status_ErrorMessage(&status));
        upb_DefPool_Free(pool);
        upb_Arena_Free(arena);
        return 1;
    }
    
    // 6. Look up Message
    const upb_MessageDef* msg_def = upb_DefPool_FindMessageByName(pool, "Person");
    if (!msg_def) {
        printf("Failed to find message 'Person' in pool\n");
        
        // Debug: Print all messages in the file
        int count = upb_FileDef_TopLevelMessageCount(file);
        printf("File has %d top-level messages:\n", count);
        for (int i = 0; i < count; i++) {
            const upb_MessageDef* m = upb_FileDef_TopLevelMessage(file, i);
            printf("  Message %d: %s\n", i, upb_MessageDef_FullName(m));
        }
        
        upb_DefPool_Free(pool);
        upb_Arena_Free(arena);
        return 1;
    }
    
    printf("Successfully parsed schema and found message 'Person'!\n");

    upb_DefPool_Free(pool);
    upb_Arena_Free(arena);
    return 0;
}
