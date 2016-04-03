/*student.c*/
#include <avro.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

avro_schema_t student_schema;
/*id������Ӽ�¼ʱΪѧ������ѧ��*/
int64_t id =0;

/*����ѧ��ģʽ��ӵ���ֶ�ѧ�š�������ѧԺ���绰������*/
#define STUDENT_SCHEMA \
"{\"type\":\"record\",\
  \"name\":\"Student\",\
  \"fields\":[\
      {\"name\": \"SID\", \"type\": \"long\"},\
      {\"name\": \"Name\", \"type\": \"string\"},\
      {\"name\": \"Dept\", \"type\": \"string\"},\
      {\"name\": \"Phone\", \"type\": \"string\"},\
      {\"name\": \"Age\", \"type\": \"int\"}]}"

/*��JSON�����ģʽ������ģʽ�����ݽṹ*/
void init(void)
{
      avro_schema_error_t error;
      if(avro_schema_from_json(STUDENT_SCHEMA,
sizeof(STUDENT_SCHEMA),
&student_schema,&error)){
              fprintf(stderr,"Failed to parse student schema\n");
              exit(EXIT_FAILURE);
       }
}

/*���ѧ����¼*/
void add_student(avro_file_writer_t db, const char *name, const char *dept, const char *phone, int32_t age)
{
	avro_datum_t student = avro_record("Student", NULL);

        avro_datum_t sid_datum = avro_int64(++id);
        avro_datum_t name_datum = avro_string(name);
        avro_datum_t dept_datum = avro_string(dept);
        avro_datum_t age_datum = avro_int32(age);
        avro_datum_t phone_datum = avro_string(phone);

     /*����ѧ����¼*/
	if (avro_record_set(student, "SID", sid_datum)
            || avro_record_set(student, "Name", name_datum)
            || avro_record_set(student, "Dept", dept_datum)
            || avro_record_set(student, "Age", age_datum)
            || avro_record_set(student, "Phone", phone_datum)) {
        fprintf(stderr, "Failed to create student datum structure");
        exit(EXIT_FAILURE);
        }
    
      /*����¼��ӵ����ݿ��ļ���*/
	 if (avro_file_writer_append(db, student)) {
         fprintf(stderr, "Failed to add student datum to database");
         exit(EXIT_FAILURE);
        }
	
     /*������ã��ͷ��ڴ�ռ�*/
	avro_datum_decref(sid_datum);
     avro_datum_decref(name_datum);
     avro_datum_decref(dept_datum);
     avro_datum_decref(age_datum);
     avro_datum_decref(phone_datum);
     avro_datum_decref(student);

     fprintf(stdout, "Successfully added %s\n", name);
}

/*������ݿ��е�ѧ����Ϣ*/
int show_student(avro_file_reader_t db, 
avro_schema_t reader_schema)
{
        int rval;
        avro_datum_t student;

        rval = avro_file_reader_read(db, reader_schema, &student);

        if (rval == 0) {
             int64_t i64;
             int32_t i32;
             char *p;
             avro_datum_t sid_datum, name_datum, dept_datum,
phone_datum, age_datum;

            if (avro_record_get(student, "SID", &sid_datum) == 0) {
                        avro_int64_get(sid_datum, &i64);
                        fprintf(stdout, "%"PRId64"  ", i64);
                }
          if (avro_record_get(student, "Name", &name_datum) == 0) {
                        avro_string_get(name_datum, &p);
                        fprintf(stdout, "%12s  ", p);
                }
          if (avro_record_get(student, "Dept", &dept_datum) == 0) {
                        avro_string_get(dept_datum, &p);
                        fprintf(stdout, "%12s  ", p);
                }
       if (avro_record_get(student, "Phone", &phone_datum) == 0) {
                        avro_string_get(phone_datum, &p);
                        fprintf(stdout, "%12s  ", p);
                }
            if (avro_record_get(student, "Age", &age_datum) == 0) {
                        avro_int32_get(age_datum, &i32);
                        fprintf(stdout, "%d", i32);
                }
                fprintf(stdout, "\n");

                /*�ͷż�¼*/
                avro_datum_decref(student);
        }
        return rval;
}

int main(void)
{
        int rval;
        avro_file_reader_t dbreader;
        avro_file_writer_t db;
        avro_schema_t extraction_schema, name_schema,
phone_schema;
        int64_t i;
        const char *dbname = "student.db";

        init();

        /*���student.db���ڣ���ɾ��*/
        unlink(dbname);
        /*�������ݿ��ļ�*/
        rval = avro_file_writer_create(dbname, student_schema, &db);
        if (rval) {
                fprintf(stderr, "Failed to create %s\n", dbname);
                exit(EXIT_FAILURE);
        }

        /*�����ݿ��ļ������ѧ����Ϣ*/
        add_student(db, "Zhanghua", "Law", "15201161111", 25);
        add_student(db, "Lili", "Economy", "15201162222", 24);
        add_student(db,"Wangyu","Information","15201163333", 25);
        add_student(db, "Zhaoxin", "Art", "15201164444", 23);
        add_student(db, "Sunqin", "Physics", "15201165555", 25);
        add_student(db, "Zhouping", "Math", "15201166666", 23);
        avro_file_writer_close(db);

        fprintf(stdout, "\nPrint all the records from database\n");

        /*��ȡ��������е�ѧ����Ϣ*/
        avro_file_reader(dbname, &dbreader);
        for (i = 0; i < id; i++) {
                if (show_student(dbreader, NULL)) {
                        fprintf(stderr, "Error printing student\n");
                        exit(EXIT_FAILURE);
                }
        }
        avro_file_reader_close(dbreader);

        /*���ѧ���������͵绰��Ϣ*/
        extraction_schema = avro_schema_record("Student", NULL);
        name_schema = avro_schema_string();
        phone_schema = avro_schema_string();
        avro_schema_record_field_append(extraction_schema,
"Name", name_schema);
        avro_schema_record_field_append(extraction_schema, "Phone", phone_schema);

        /*ֻ��ȡÿ��ѧ���������͵绰*/
        fprintf(stdout,
                "\n\nExtract Name & Phone of the records from database\n");
        avro_file_reader(dbname, &dbreader);
        for (i = 0; i < id; i++) {
                if (show_student(dbreader, extraction_schema)) {
                        fprintf(stderr, "Error printing student\n");
                        exit(EXIT_FAILURE);
                }
        }
        avro_file_reader_close(dbreader);
        avro_schema_decref(name_schema);
        avro_schema_decref(phone_schema);
        avro_schema_decref(extraction_schema);

        /*����ͷ�ѧ��ģʽ*/
        avro_schema_decref(student_schema);
        return 0;
}
