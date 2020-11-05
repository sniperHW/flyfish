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

INSERT INTO "table_conf" (__table__,__conf__) VALUES ('users1', 'name:string:,age:int:,phone:string:');
DROP TABLE IF EXISTS "users1";
CREATE TABLE "users1" (
    "__key__" varchar(255) NOT NULL,
    "__version__" int8 NOT NULL,
    "name" varchar(255) NOT NULL,
    "age" int8 NOT NULL DEFAULT 0,
    "phone" text NOT NULL,
    PRIMARY KEY ("__key__")
);



DELETE FROM "table_conf" WHERE __table__ = 'test_indecr';
DROP TABLE IF EXISTS "test_indecr";

DELETE FROM "table_conf" WHERE __table__ = 'users1';
DROP TABLE IF EXISTS "users1";