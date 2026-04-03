#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "upb_out/upb.h"

typedef struct lite_proto_vtab {
  sqlite3_vtab base;
  upb_DefPool *pool;
  const upb_MessageDef *msg_def;
  char *base_table;
  char *base_column;
  sqlite3 *db;
} lite_proto_vtab;

typedef struct lite_proto_cursor {
  sqlite3_vtab_cursor base;
  upb_Message *msg;
  upb_Arena *arena;
  int eof;
  sqlite3_stmt *stmt; // For scanning base table
} lite_proto_cursor;

static int load_schema(
  const char *pb_path,
  const char *msg_name,
  upb_DefPool **pPool,
  const upb_MessageDef **pMsgDef,
  upb_Arena **pArena,
  char **pzErr
){
  FILE* f = fopen(pb_path, "rb");
  if (!f) {
    *pzErr = sqlite3_mprintf("Failed to open schema file: %s", pb_path);
    return SQLITE_ERROR;
  }
  
  fseek(f, 0, SEEK_END);
  long size = ftell(f);
  fseek(f, 0, SEEK_SET);
  
  char* buf = malloc(size);
  if (!buf) {
    fclose(f);
    return SQLITE_NOMEM; // LCOV_EXCL_LINE OOM handler
  }
  
  if (fread(buf, 1, size, f) != size) {
    *pzErr = sqlite3_mprintf("Failed to read schema file");
    free(buf);
    fclose(f);
    return SQLITE_ERROR;
  }
  fclose(f);
  
  *pArena = upb_Arena_New();
  *pPool = upb_DefPool_New();
  
  google_protobuf_FileDescriptorSet* set = google_protobuf_FileDescriptorSet_parse(buf, size, *pArena);
  free(buf);
  
  if (!set) {
    *pzErr = sqlite3_mprintf("Failed to parse FileDescriptorSet");
    upb_DefPool_Free(*pPool);
    upb_Arena_Free(*pArena);
    return SQLITE_ERROR;
  }
  
  size_t file_count;
  const google_protobuf_FileDescriptorProto* const* files = google_protobuf_FileDescriptorSet_file(set, &file_count);
  if (file_count == 0) {
    *pzErr = sqlite3_mprintf("No files in FileDescriptorSet");
    upb_DefPool_Free(*pPool);
    upb_Arena_Free(*pArena);
    return SQLITE_ERROR;
  }
  
  upb_Status status;
  upb_Status_Clear(&status);
  const upb_FileDef* file_def = upb_DefPool_AddFile(*pPool, files[0], &status);
  if (!file_def) {
    *pzErr = sqlite3_mprintf("Failed to add file to pool: %s", upb_Status_ErrorMessage(&status)); // LCOV_EXCL_LINE
    upb_DefPool_Free(*pPool); // LCOV_EXCL_LINE
    upb_Arena_Free(*pArena); // LCOV_EXCL_LINE
    return SQLITE_ERROR; // LCOV_EXCL_LINE
  }
  
  *pMsgDef = upb_DefPool_FindMessageByName(*pPool, msg_name);
  if (!*pMsgDef) {
    *pzErr = sqlite3_mprintf("Failed to find message: %s", msg_name); // LCOV_EXCL_LINE
    upb_DefPool_Free(*pPool); // LCOV_EXCL_LINE
    upb_Arena_Free(*pArena); // LCOV_EXCL_LINE
    return SQLITE_ERROR; // LCOV_EXCL_LINE
  }
  
  return SQLITE_OK;
}

static void strip_quotes(const char *arg, char *buf, size_t buf_size) {
  size_t len = strlen(arg);
  if (len > 1 && ((arg[0] == '\'' && arg[len-1] == '\'') ||
                  (arg[0] == '"' && arg[len-1] == '"'))) {
    size_t copy_len = len - 2;
    if (copy_len >= buf_size) copy_len = buf_size - 1;
    strncpy(buf, arg + 1, copy_len);
    buf[copy_len] = '\0';
  } else {
    strncpy(buf, arg, buf_size - 1);
    buf[buf_size - 1] = '\0';
  }
}

static int liteProtoCreate(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  if (argc < 5) {
    *pzErr = sqlite3_mprintf("Usage: CREATE VIRTUAL TABLE name USING lite_proto('path/to/schema.pb', 'MessageName'[, 'base_table', 'base_column'])");
    return SQLITE_ERROR;
  }
  
  char pb_path[256];
  char msg_name[256];
  char base_table[256] = "";
  char base_column[256] = "";
  
  strip_quotes(argv[3], pb_path, sizeof(pb_path));
  strip_quotes(argv[4], msg_name, sizeof(msg_name));
  
  if (argc >= 7) {
    strip_quotes(argv[5], base_table, sizeof(base_table));
    strip_quotes(argv[6], base_column, sizeof(base_column));
  }
  
  // Read file
  FILE* f = fopen(pb_path, "rb");
  if (!f) {
    *pzErr = sqlite3_mprintf("Failed to open schema file: %s (arg was: %s)", pb_path, argv[3]);
    return SQLITE_ERROR;
  }
  
  fseek(f, 0, SEEK_END);
  long size = ftell(f);
  fseek(f, 0, SEEK_SET);
  
  char* buf = malloc(size);
  if (!buf) {
    fclose(f);
    return SQLITE_NOMEM; // LCOV_EXCL_LINE OOM handler
  }
  
  if (fread(buf, 1, size, f) != size) {
    *pzErr = sqlite3_mprintf("Failed to read schema file"); // LCOV_EXCL_LINE
    free(buf); // LCOV_EXCL_LINE
    fclose(f); // LCOV_EXCL_LINE
    return SQLITE_ERROR; // LCOV_EXCL_LINE
  }
  fclose(f);
  
  upb_Arena* arena = upb_Arena_New();
  upb_DefPool* pool = upb_DefPool_New();
  
  google_protobuf_FileDescriptorSet* set = google_protobuf_FileDescriptorSet_parse(buf, size, arena);
  free(buf);
  
  if (!set) {
    *pzErr = sqlite3_mprintf("Failed to parse FileDescriptorSet"); // LCOV_EXCL_LINE
    upb_DefPool_Free(pool); // LCOV_EXCL_LINE
    upb_Arena_Free(arena); // LCOV_EXCL_LINE
    return SQLITE_ERROR; // LCOV_EXCL_LINE
  }
  
  size_t file_count;
  const google_protobuf_FileDescriptorProto* const* files = google_protobuf_FileDescriptorSet_file(set, &file_count);
  if (file_count == 0) {
    *pzErr = sqlite3_mprintf("No files in FileDescriptorSet"); // LCOV_EXCL_LINE
    upb_DefPool_Free(pool); // LCOV_EXCL_LINE
    upb_Arena_Free(arena); // LCOV_EXCL_LINE
    return SQLITE_ERROR; // LCOV_EXCL_LINE
  }
  
  upb_Status status;
  upb_Status_Clear(&status);
  const upb_FileDef* file_def = upb_DefPool_AddFile(pool, files[0], &status);
  if (!file_def) {
    *pzErr = sqlite3_mprintf("Failed to add file to pool: %s", upb_Status_ErrorMessage(&status)); // LCOV_EXCL_LINE
    upb_DefPool_Free(pool); // LCOV_EXCL_LINE
    upb_Arena_Free(arena); // LCOV_EXCL_LINE
    return SQLITE_ERROR; // LCOV_EXCL_LINE
  }
  
  const upb_MessageDef* msg_def = upb_DefPool_FindMessageByName(pool, msg_name);
  if (!msg_def) {
    *pzErr = sqlite3_mprintf("Failed to find message: %s", msg_name); // LCOV_EXCL_LINE
    upb_DefPool_Free(pool); // LCOV_EXCL_LINE
    upb_Arena_Free(arena); // LCOV_EXCL_LINE
    return SQLITE_ERROR; // LCOV_EXCL_LINE
  }
  
  // Build CREATE TABLE string
  char sql[1024];
  strcpy(sql, "CREATE TABLE x(");
  
  int field_count = upb_MessageDef_FieldCount(msg_def);
  for (int i = 0; i < field_count; i++) {
    const upb_FieldDef* fdef = upb_MessageDef_Field(msg_def, i);
    const char* name = upb_FieldDef_Name(fdef);
    upb_CType ctype = upb_FieldDef_CType(fdef);
    
    strcat(sql, name);
    strcat(sql, " ");
    
    switch (ctype) {
      case kUpb_CType_Bool:
      case kUpb_CType_Int32:
      case kUpb_CType_UInt32:
      case kUpb_CType_Int64:
      case kUpb_CType_UInt64:
      case kUpb_CType_Enum:
        strcat(sql, "INTEGER");
        break;
      case kUpb_CType_Float:
      case kUpb_CType_Double:
        strcat(sql, "REAL");
        break;
      case kUpb_CType_String:
        strcat(sql, "TEXT");
        break;
      default:
        strcat(sql, "BLOB");
    }
    
    if (i < field_count - 1) {
      strcat(sql, ", ");
    }
  }
  strcat(sql, ", _blob BLOB HIDDEN)");
  
  int rc = sqlite3_declare_vtab(db, sql);
  if (rc != SQLITE_OK) {
    upb_DefPool_Free(pool); // LCOV_EXCL_LINE
    upb_Arena_Free(arena); // LCOV_EXCL_LINE
    return rc; // LCOV_EXCL_LINE
  }
  
  lite_proto_vtab *pVtab = sqlite3_malloc(sizeof(lite_proto_vtab));
  if( pVtab==0 ) {
    upb_DefPool_Free(pool);
    upb_Arena_Free(arena);
    return SQLITE_NOMEM; // LCOV_EXCL_LINE OOM handler
  }
  memset(pVtab, 0, sizeof(lite_proto_vtab));
  pVtab->pool = pool;
  pVtab->msg_def = msg_def;
  pVtab->db = db;
  if (strlen(base_table) > 0) {
    pVtab->base_table = sqlite3_mprintf("%s", base_table);
    pVtab->base_column = sqlite3_mprintf("%s", base_column);
  }
  *ppVtab = &pVtab->base;
  
  upb_Arena_Free(arena);
  
  return SQLITE_OK;
}

static int liteProtoConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  return liteProtoCreate(db, pAux, argc, argv, ppVtab, pzErr);
}

static int liteProtoDisconnect(sqlite3_vtab *pVtab){
  lite_proto_vtab *p = (lite_proto_vtab*)pVtab;
  if (p->pool) {
    upb_DefPool_Free(p->pool);
  }
  if (p->base_table) sqlite3_free(p->base_table);
  if (p->base_column) sqlite3_free(p->base_column);
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int liteProtoDestroy(sqlite3_vtab *pVtab){
  return liteProtoDisconnect(pVtab);
}

static int liteProtoOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  lite_proto_cursor *pCur = sqlite3_malloc(sizeof(lite_proto_cursor));
  if( pCur==0 ) return SQLITE_NOMEM; // LCOV_EXCL_LINE OOM handler
  memset(pCur, 0, sizeof(lite_proto_cursor));
  pCur->arena = upb_Arena_New();
  if (pCur->arena == 0) {
    sqlite3_free(pCur);
    return SQLITE_NOMEM; // LCOV_EXCL_LINE OOM handler
  }
  pCur->eof = 1; // Start as EOF until Filter is called
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

static int liteProtoClose(sqlite3_vtab_cursor *pCursor){
  lite_proto_cursor *pCur = (lite_proto_cursor*)pCursor;
  if (pCur->stmt) {
    sqlite3_finalize(pCur->stmt);
  }
  if (pCur->arena) {
    upb_Arena_Free(pCur->arena);
  }
  sqlite3_free(pCursor);
  return SQLITE_OK;
}

static int liteProtoFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  lite_proto_cursor *pCur = (lite_proto_cursor*)pVtabCursor;
  lite_proto_vtab *pVtab = (lite_proto_vtab*)pVtabCursor->pVtab;
  
  if (idxNum == 1 && argc > 0) {
    const void* blob_data = sqlite3_value_blob(argv[0]);
    int blob_size = sqlite3_value_bytes(argv[0]);
    
    const upb_MiniTable* layout = upb_MessageDef_MiniTable(pVtab->msg_def);
    pCur->msg = upb_Message_New(layout, pCur->arena);
    
    upb_DecodeStatus status = upb_Decode(blob_data, blob_size, pCur->msg, layout, NULL, 0, pCur->arena);
    if (status != kUpb_DecodeStatus_Ok) {
      pVtab->base.zErrMsg = sqlite3_mprintf("Failed to decode protobuf: %d", status); // LCOV_EXCL_LINE
      pCur->eof = 1; // LCOV_EXCL_LINE
      return SQLITE_ERROR; // LCOV_EXCL_LINE
    }
    pCur->eof = 0; // Found 1 row
  } else if (pVtab->base_table && pVtab->base_column) {
    // No blob provided, but we have a base table. Scan it.
    char sql[512];
    snprintf(sql, sizeof(sql), "SELECT %s FROM %s", pVtab->base_column, pVtab->base_table);
    
    int rc = sqlite3_prepare_v2(pVtab->db, sql, -1, &pCur->stmt, NULL);
    if (rc != SQLITE_OK) {
      pVtab->base.zErrMsg = sqlite3_mprintf("Failed to prepare scan statement: %s", sqlite3_errmsg(pVtab->db)); // LCOV_EXCL_LINE
      pCur->eof = 1; // LCOV_EXCL_LINE
      return rc; // LCOV_EXCL_LINE
    }
    
    // Get first row
    rc = sqlite3_step(pCur->stmt);
    if (rc == SQLITE_ROW) {
      const void* blob_data = sqlite3_column_blob(pCur->stmt, 0);
      int blob_size = sqlite3_column_bytes(pCur->stmt, 0);
      
      const upb_MiniTable* layout = upb_MessageDef_MiniTable(pVtab->msg_def);
      pCur->msg = upb_Message_New(layout, pCur->arena);
      
      upb_DecodeStatus status = upb_Decode(blob_data, blob_size, pCur->msg, layout, NULL, 0, pCur->arena);
      if (status != kUpb_DecodeStatus_Ok) {
        pVtab->base.zErrMsg = sqlite3_mprintf("Failed to decode protobuf: %d", status); // LCOV_EXCL_LINE
        pCur->eof = 1; // LCOV_EXCL_LINE
        sqlite3_finalize(pCur->stmt); // LCOV_EXCL_LINE
        pCur->stmt = NULL; // LCOV_EXCL_LINE
        return SQLITE_ERROR; // LCOV_EXCL_LINE
      }
      pCur->eof = 0;
    } else if (rc == SQLITE_DONE) {
      pCur->eof = 1;
      sqlite3_finalize(pCur->stmt);
      pCur->stmt = NULL;
    } else {
      pVtab->base.zErrMsg = sqlite3_mprintf("Failed to execute scan: %s", sqlite3_errmsg(pVtab->db)); // LCOV_EXCL_LINE
      pCur->eof = 1; // LCOV_EXCL_LINE
      sqlite3_finalize(pCur->stmt); // LCOV_EXCL_LINE
      pCur->stmt = NULL; // LCOV_EXCL_LINE
      return SQLITE_ERROR; // LCOV_EXCL_LINE
    }
  } else {
    pCur->eof = 1; // No blob provided, and no base table
  }
  
  return SQLITE_OK;
}

static int liteProtoNext(sqlite3_vtab_cursor *pVtabCursor){
  lite_proto_cursor *pCur = (lite_proto_cursor*)pVtabCursor;
  lite_proto_vtab *pVtab = (lite_proto_vtab*)pVtabCursor->pVtab;
  
  if (pCur->stmt) {
    int rc = sqlite3_step(pCur->stmt);
    if (rc == SQLITE_ROW) {
      const void* blob_data = sqlite3_column_blob(pCur->stmt, 0);
      int blob_size = sqlite3_column_bytes(pCur->stmt, 0);
      
      const upb_MiniTable* layout = upb_MessageDef_MiniTable(pVtab->msg_def);
      
      upb_Arena_Free(pCur->arena);
      pCur->arena = upb_Arena_New();
      pCur->msg = upb_Message_New(layout, pCur->arena);
      
      upb_DecodeStatus status = upb_Decode(blob_data, blob_size, pCur->msg, layout, NULL, 0, pCur->arena);
      if (status != kUpb_DecodeStatus_Ok) {
        pVtab->base.zErrMsg = sqlite3_mprintf("Failed to decode protobuf: %d", status); // LCOV_EXCL_LINE
        pCur->eof = 1; // LCOV_EXCL_LINE
        sqlite3_finalize(pCur->stmt); // LCOV_EXCL_LINE
        pCur->stmt = NULL; // LCOV_EXCL_LINE
        return SQLITE_ERROR; // LCOV_EXCL_LINE
      }
      pCur->eof = 0;
    } else if (rc == SQLITE_DONE) {
      pCur->eof = 1;
      sqlite3_finalize(pCur->stmt);
      pCur->stmt = NULL;
    } else {
      pVtab->base.zErrMsg = sqlite3_mprintf("Failed to execute scan: %s", sqlite3_errmsg(pVtab->db)); // LCOV_EXCL_LINE
      pCur->eof = 1; // LCOV_EXCL_LINE
      sqlite3_finalize(pCur->stmt); // LCOV_EXCL_LINE
      pCur->stmt = NULL; // LCOV_EXCL_LINE
      return SQLITE_ERROR; // LCOV_EXCL_LINE
    }
  } else {
    pCur->eof = 1; // We only support 1 row per blob for now
  }
  return SQLITE_OK;
}

static int liteProtoEof(sqlite3_vtab_cursor *pVtabCursor){
  lite_proto_cursor *pCur = (lite_proto_cursor*)pVtabCursor;
  return pCur->eof;
}

static int liteProtoColumn(
  sqlite3_vtab_cursor *pVtabCursor,
  sqlite3_context *ctx,
  int i
){
  lite_proto_cursor *pCur = (lite_proto_cursor*)pVtabCursor;
  lite_proto_vtab *pVtab = (lite_proto_vtab*)pVtabCursor->pVtab;
  
  int field_count = upb_MessageDef_FieldCount(pVtab->msg_def);
  
  if (i < field_count) {
    const upb_FieldDef* fdef = upb_MessageDef_Field(pVtab->msg_def, i);
    
    if (!pCur->msg) {
      sqlite3_result_null(ctx); // LCOV_EXCL_LINE Unreachable
      return SQLITE_OK;
    }
    
    if (upb_FieldDef_IsRepeated(fdef)) {
      sqlite3_result_text(ctx, "[Repeated]", -1, SQLITE_STATIC);
      return SQLITE_OK;
    }
    
    upb_MessageValue val = upb_Message_GetFieldByDef(pCur->msg, fdef);
    upb_CType ctype = upb_FieldDef_CType(fdef);
    
    switch (ctype) {
      case kUpb_CType_Bool:
        sqlite3_result_int(ctx, val.bool_val);
        break;
      case kUpb_CType_Int32:
      case kUpb_CType_Enum:
        sqlite3_result_int(ctx, val.int32_val);
        break;
      case kUpb_CType_UInt32:
        sqlite3_result_int64(ctx, val.uint32_val);
        break;
      case kUpb_CType_Int64:
        sqlite3_result_int64(ctx, val.int64_val);
        break;
      case kUpb_CType_UInt64:
        sqlite3_result_int64(ctx, (sqlite3_int64)val.uint64_val);
        break;
      case kUpb_CType_Float:
        sqlite3_result_double(ctx, val.float_val);
        break;
      case kUpb_CType_Double:
        sqlite3_result_double(ctx, val.double_val);
        break;
      case kUpb_CType_String: {
        upb_StringView sv = val.str_val;
        sqlite3_result_text(ctx, sv.data, sv.size, SQLITE_TRANSIENT);
        break;
      }
      case kUpb_CType_Bytes: {
        upb_StringView sv = val.str_val;
        sqlite3_result_blob(ctx, sv.data, sv.size, SQLITE_TRANSIENT);
        break;
      }
      default:
        sqlite3_result_null(ctx);
    }
  } else if (i == field_count) {
    sqlite3_result_null(ctx);
  }
  
  return SQLITE_OK;
}

static int liteProtoRowid(sqlite3_vtab_cursor *pVtabCursor, sqlite3_int64 *pRowid){
  *pRowid = 1;
  return SQLITE_OK;
}

static int liteProtoBestIndex(
  sqlite3_vtab *pVTab,
  sqlite3_index_info *pIdxInfo
){
  lite_proto_vtab *pVtab = (lite_proto_vtab*)pVTab;
  int field_count = upb_MessageDef_FieldCount(pVtab->msg_def);
  
  for (int i = 0; i < pIdxInfo->nConstraint; i++) {
    if (pIdxInfo->aConstraint[i].iColumn == field_count && // _blob column
        pIdxInfo->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_EQ &&
        pIdxInfo->aConstraint[i].usable) {
      
      pIdxInfo->aConstraintUsage[i].argvIndex = 1;
      pIdxInfo->aConstraintUsage[i].omit = 1; // We handle this constraint
      
      pIdxInfo->estimatedCost = 1;
      pIdxInfo->idxNum = 1; // Signal to xFilter that we have the blob
      return SQLITE_OK;
    }
  }
  
  pIdxInfo->estimatedCost = 1000000; // Expensive if no blob provided
  return SQLITE_OK;
}

static int liteProtoUpdate(
  sqlite3_vtab *pVtab,
  int argc,
  sqlite3_value **argv,
  sqlite_int64 *pRowid
){
  lite_proto_vtab *p = (lite_proto_vtab*)pVtab;
  
  if (argc == 1) {
    // DELETE
    pVtab->zErrMsg = sqlite3_mprintf("DELETE not supported yet");
    return SQLITE_ERROR;
  }
  
  if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
    // INSERT
    if (!p->base_table || !p->base_column) {
      pVtab->zErrMsg = sqlite3_mprintf("Virtual table is read-only (base table/column not specified)");
      return SQLITE_ERROR;
    }
    
    // Create new message
    upb_Arena *arena = upb_Arena_New();
    const upb_MiniTable *layout = upb_MessageDef_MiniTable(p->msg_def);
    upb_Message *msg = upb_Message_New(layout, arena);
    
    int field_count = upb_MessageDef_FieldCount(p->msg_def);
    for (int i = 0; i < field_count; i++) {
      const upb_FieldDef* fdef = upb_MessageDef_Field(p->msg_def, i);
      sqlite3_value *val = argv[2 + i];
      
      if (sqlite3_value_type(val) == SQLITE_NULL) continue;
      
      upb_CType ctype = upb_FieldDef_CType(fdef);
      upb_MessageValue u_val;
      
      switch (ctype) {
        case kUpb_CType_Bool:
          u_val.bool_val = sqlite3_value_int(val);
          break;
        case kUpb_CType_Int32:
          u_val.int32_val = sqlite3_value_int(val);
          break;
        case kUpb_CType_UInt32:
          u_val.uint32_val = sqlite3_value_int64(val);
          break;
        case kUpb_CType_Int64:
          u_val.int64_val = sqlite3_value_int64(val);
          break;
        case kUpb_CType_UInt64:
          u_val.uint64_val = sqlite3_value_int64(val);
          break;
        case kUpb_CType_String: {
          const char *str = (const char*)sqlite3_value_text(val);
          u_val.str_val.data = str;
          u_val.str_val.size = strlen(str);
          break;
        }
        default:
          // Unsupported for now
          continue;
      }
      upb_Message_SetFieldByDef(msg, fdef, u_val, arena);
    }
    
    // Encode message
    char *buf = NULL;
    size_t size = 0;
    upb_EncodeStatus status = upb_Encode(msg, layout, 0, arena, &buf, &size);
    
    if (status != kUpb_EncodeStatus_Ok) {
      pVtab->zErrMsg = sqlite3_mprintf("Failed to encode protobuf message: %d", status); // LCOV_EXCL_LINE
      upb_Arena_Free(arena); // LCOV_EXCL_LINE
      return SQLITE_ERROR; // LCOV_EXCL_LINE
    }
    
    // Insert into base table
    char sql[512];
    snprintf(sql, sizeof(sql), "INSERT INTO %s (%s) VALUES (?)", p->base_table, p->base_column);
    
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(p->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
      pVtab->zErrMsg = sqlite3_mprintf("Failed to prepare insert statement: %s", sqlite3_errmsg(p->db)); // LCOV_EXCL_LINE
      upb_Arena_Free(arena); // LCOV_EXCL_LINE
      return rc; // LCOV_EXCL_LINE
    }
    
    sqlite3_bind_blob(stmt, 1, buf, size, SQLITE_TRANSIENT);
    
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    
    if (rc != SQLITE_DONE) {
      pVtab->zErrMsg = sqlite3_mprintf("Failed to execute insert: %s", sqlite3_errmsg(p->db)); // LCOV_EXCL_LINE
      upb_Arena_Free(arena); // LCOV_EXCL_LINE
      return SQLITE_ERROR; // LCOV_EXCL_LINE
    }
    
    *pRowid = sqlite3_last_insert_rowid(p->db);
    
    upb_Arena_Free(arena);
    return SQLITE_OK;
  }
  
  pVtab->zErrMsg = sqlite3_mprintf("UPDATE not supported yet");
  return SQLITE_ERROR;
}

static sqlite3_module liteProtoModule = {
  0,                         /* iVersion */
  liteProtoCreate,           /* xCreate */
  liteProtoConnect,          /* xConnect */
  liteProtoBestIndex,        /* xBestIndex */
  liteProtoDisconnect,       /* xDisconnect */
  liteProtoDestroy,          /* xDestroy */
  liteProtoOpen,             /* xOpen */
  liteProtoClose,            /* xClose */
  liteProtoFilter,           /* xFilter */
  liteProtoNext,             /* xNext */
  liteProtoEof,              /* xEof */
  liteProtoColumn,           /* xColumn */
  liteProtoRowid,            /* xRowid */
  liteProtoUpdate,           /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindFunction */
  0,                         /* xRename */
};

typedef struct CachedSchema {
  upb_DefPool *pool;
  const upb_MessageDef *msg_def;
  upb_Arena *arena;
} CachedSchema;

static void freeCachedSchema(void *p) {
  CachedSchema *cs = (CachedSchema*)p;
  if (cs) {
    if (cs->pool) upb_DefPool_Free(cs->pool);
    if (cs->arena) upb_Arena_Free(cs->arena);
    free(cs);
  }
}

static void proto_extract_func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  if (argc < 4) {
    sqlite3_result_error(context, "proto_extract requires 4 arguments", -1);
    return;
  }
  
  sqlite3_value *blob_val = argv[0];
  const char *pb_path = (const char*)sqlite3_value_text(argv[1]);
  const char *msg_name = (const char*)sqlite3_value_text(argv[2]);
  const char *field_name = (const char*)sqlite3_value_text(argv[3]);
  
  if (!pb_path || !msg_name || !field_name) {
    sqlite3_result_error(context, "Invalid arguments", -1); // LCOV_EXCL_LINE
    return; // LCOV_EXCL_LINE
  }
  
  if (sqlite3_value_type(blob_val) != SQLITE_BLOB) {
    sqlite3_result_null(context); // LCOV_EXCL_LINE
    return; // LCOV_EXCL_LINE
  }
  
  const void *blob_data = sqlite3_value_blob(blob_val);
  int blob_size = sqlite3_value_bytes(blob_val);
  
  CachedSchema *cs = sqlite3_get_auxdata(context, 1); // Bind to 2nd argument (pb_path)
  
  if (!cs) {
    cs = malloc(sizeof(CachedSchema));
    if (!cs) {
      sqlite3_result_error_nomem(context); // LCOV_EXCL_LINE
      return;
    }
    cs->pool = NULL;
    cs->msg_def = NULL;
    cs->arena = NULL;
    
    char *zErr = NULL;
    int rc = load_schema(pb_path, msg_name, &cs->pool, &cs->msg_def, &cs->arena, &zErr);
    if (rc != SQLITE_OK) {
      sqlite3_result_error(context, zErr ? zErr : "Failed to load schema", -1);
      if (zErr) sqlite3_free(zErr);
      freeCachedSchema(cs);
      return;
    }
    
    sqlite3_set_auxdata(context, 1, cs, freeCachedSchema);
  }
  
  upb_Arena *arena = upb_Arena_New();
  const upb_MiniTable* layout = upb_MessageDef_MiniTable(cs->msg_def);
  upb_Message *msg = upb_Message_New(layout, arena);
  
  if (upb_Decode(blob_data, blob_size, msg, layout, NULL, 0, arena) != kUpb_DecodeStatus_Ok) {
    upb_Arena_Free(arena);
    sqlite3_result_error(context, "Failed to decode protobuf message", -1);
    return;
  }
  
  char *path = strdup(field_name);
  char *token = strtok(path, ".");
  const upb_MessageDef *curr_mdef = cs->msg_def;
  upb_Message *curr_msg = msg;
  const upb_FieldDef *fdef = NULL;
  
  while (token != NULL) {
    fdef = upb_MessageDef_FindFieldByName(curr_mdef, token);
    if (!fdef) {
      sqlite3_result_null(context);
      free(path);
      upb_Arena_Free(arena);
      return;
    }
    
    token = strtok(NULL, ".");
    if (token != NULL) {
      if (upb_FieldDef_CType(fdef) != kUpb_CType_Message) {
        sqlite3_result_null(context);
        free(path);
        upb_Arena_Free(arena);
        return;
      }
      
      upb_MessageValue val = upb_Message_GetFieldByDef(curr_msg, fdef);
      curr_msg = (upb_Message*)val.msg_val;
      if (!curr_msg) {
        sqlite3_result_null(context);
        free(path);
        upb_Arena_Free(arena);
        return;
      }
      curr_mdef = upb_FieldDef_MessageSubDef(fdef);
    }
  }
  free(path);
  
  upb_MessageValue val = upb_Message_GetFieldByDef(curr_msg, fdef);
  upb_CType ctype = upb_FieldDef_CType(fdef);
  
  if (upb_FieldDef_IsRepeated(fdef)) {
    upb_Array *arr = (upb_Array*)val.array_val;
    if (!arr) {
      sqlite3_result_null(context);
      upb_Arena_Free(arena);
      return;
    }
    
    size_t size = upb_Array_Size(arr);
    if (size == 0) {
      sqlite3_result_text(context, "", 0, SQLITE_TRANSIENT); // LCOV_EXCL_LINE
      upb_Arena_Free(arena); // LCOV_EXCL_LINE
      return; // LCOV_EXCL_LINE
    }
    
    sqlite3_str *str = sqlite3_str_new(sqlite3_context_db_handle(context));
    
    for (size_t i = 0; i < size; i++) {
      upb_MessageValue item_val = upb_Array_Get(arr, i);
      if (i > 0) sqlite3_str_appendall(str, ",");
      
      switch (ctype) {
        case kUpb_CType_Bool:
          sqlite3_str_appendf(str, "%d", item_val.bool_val);
          break;
        case kUpb_CType_Int32:
          sqlite3_str_appendf(str, "%d", item_val.int32_val);
          break;
        case kUpb_CType_UInt32:
          sqlite3_str_appendf(str, "%u", item_val.uint32_val);
          break;
        case kUpb_CType_Int64:
          sqlite3_str_appendf(str, "%lld", item_val.int64_val);
          break;
        case kUpb_CType_UInt64:
          sqlite3_str_appendf(str, "%llu", item_val.uint64_val);
          break;
        case kUpb_CType_Float:
          sqlite3_str_appendf(str, "%f", item_val.float_val);
          break;
        case kUpb_CType_Double:
          sqlite3_str_appendf(str, "%f", item_val.double_val);
          break;
        case kUpb_CType_String:
          sqlite3_str_append(str, item_val.str_val.data, item_val.str_val.size);
          break;
        default:
          sqlite3_str_appendall(str, "<unsupported>"); // LCOV_EXCL_LINE
          break; // LCOV_EXCL_LINE
      }
    }
    
    char *res_str = sqlite3_str_finish(str);
    if (!res_str) {
      sqlite3_result_error_nomem(context); // LCOV_EXCL_LINE
    } else {
      sqlite3_result_text(context, res_str, -1, sqlite3_free);
    }
    upb_Arena_Free(arena);
    return;
  }
  
  switch (ctype) {
    case kUpb_CType_Bool:
      sqlite3_result_int(context, val.bool_val);
      break;
    case kUpb_CType_Int32:
      sqlite3_result_int(context, val.int32_val);
      break;
    case kUpb_CType_UInt32:
      sqlite3_result_int64(context, val.uint32_val);
      break;
    case kUpb_CType_Int64:
      sqlite3_result_int64(context, val.int64_val);
      break;
    case kUpb_CType_UInt64:
      sqlite3_result_int64(context, val.uint64_val);
      break;
    case kUpb_CType_Enum:
      sqlite3_result_int(context, val.int32_val);
      break;
    case kUpb_CType_Float:
      sqlite3_result_double(context, val.float_val);
      break;
    case kUpb_CType_Double:
      sqlite3_result_double(context, val.double_val);
      break;
    case kUpb_CType_String:
      sqlite3_result_text(context, val.str_val.data, val.str_val.size, SQLITE_TRANSIENT);
      break;
    case kUpb_CType_Bytes:
      sqlite3_result_blob(context, val.str_val.data, val.str_val.size, SQLITE_TRANSIENT);
      break;
    default:
      sqlite3_result_null(context); // LCOV_EXCL_LINE
      break;
  }
  
  upb_Arena_Free(arena);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_liteproto_init(
  sqlite3 *db, 
  char **pzErr, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  
  rc = sqlite3_create_module(db, "lite_proto", &liteProtoModule, 0);
  if (rc == SQLITE_OK) {
    rc = sqlite3_create_function(db, "proto_extract", -1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, proto_extract_func, 0, 0);
  }
  return rc;
}
