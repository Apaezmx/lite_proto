#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
    sqlite3 *db;
    char *zErr = 0;
    int rc;

    sqlite3_initialize();

    rc = sqlite3_open(":memory:", &db);
    if (rc) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    sqlite3_enable_load_extension(db, 1);

    rc = sqlite3_load_extension(db, "./lite_proto.so", "sqlite3_liteproto_init", &zErr);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to load extension: %s\n", zErr);
        sqlite3_free(zErr);
        sqlite3_close(db);
        return 1;
    }

    // 1. Test basic create and destroy (without base table)
    rc = sqlite3_exec(db, "CREATE VIRTUAL TABLE v_person USING lite_proto('person.pb', 'Person')", NULL, NULL, &zErr);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create virtual table: %s\n", zErr);
        sqlite3_free(zErr);
        sqlite3_close(db);
        return 1;
    }

    rc = sqlite3_exec(db, "DROP TABLE v_person", NULL, NULL, &zErr);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to drop virtual table: %s\n", zErr);
        sqlite3_free(zErr);
        sqlite3_close(db);
        return 1;
    }

    // 2. Test read support with base table
    sqlite3_exec(db, "CREATE TABLE base_person (id INTEGER PRIMARY KEY, pb_blob BLOB)", NULL, NULL, NULL);

    FILE *f = fopen("person.bin", "rb");
    if (f) {
        fseek(f, 0, SEEK_END);
        long fsize = ftell(f);
        fseek(f, 0, SEEK_SET);

        char *buf = malloc(fsize);
        if (buf) {
            fread(buf, fsize, 1, f);
            fclose(f);

            sqlite3_stmt *stmt;
            sqlite3_prepare_v2(db, "INSERT INTO base_person (pb_blob) VALUES (?)", -1, &stmt, NULL);
            sqlite3_bind_blob(stmt, 1, buf, fsize, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
            free(buf);
        } else {
            fclose(f);
        }
    }

    rc = sqlite3_exec(db, "CREATE VIRTUAL TABLE v_person_read USING lite_proto('person.pb', 'Person', 'base_person', 'pb_blob')", NULL, NULL, &zErr);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create read virtual table: %s\n", zErr);
        sqlite3_free(zErr);
        sqlite3_close(db);
        return 1;
    }

    rc = sqlite3_exec(db, "SELECT * FROM v_person_read", NULL, NULL, &zErr);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to query virtual table: %s\n", zErr);
        sqlite3_free(zErr);
        sqlite3_close(db);
        return 1;
    }

    rc = sqlite3_exec(db, "DROP TABLE v_person_read", NULL, NULL, &zErr);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to drop read virtual table: %s\n", zErr);
        sqlite3_free(zErr);
        sqlite3_close(db);
        return 1;
    }

    sqlite3_close(db);
    sqlite3_shutdown();
    
    printf("Leak test completed successfully\n");
    return 0;
}
