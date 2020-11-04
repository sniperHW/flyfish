INSERT INTO "table_conf" (__table__,__conf__) VALUES ('test_indecr', 'n:int:10,a:string:a,b:string:b');

DROP TABLE IF EXISTS "test_indecr";
CREATE TABLE "test_indecr" (
    "__key__" varchar(255) NOT NULL,
    "__version__" int8 NOT NULL,
    "n" int8 NOT NULL DEFAULT 0,
    "a" varchar(255) NOT NULL DEFAULT 'a',
    "b" varchar(255) NOT NULL DEFAULT 'b',
    PRIMARY KEY ("__key__")
);



DELETE FROM "table_conf" WHERE __table__ = "test_indecr";
DROP TABLE IF EXISTS "test_indecr";