#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "mysql.h"

#pragma comment(lib, "libmysql.lib")

// 20200110 정태현

MYSQL_RES* sql_result;
MYSQL_ROW sql_row;

const char* host = "localhost";
const char* user = "root";
const char* pw = "1a2s3d4f";
const char* db = "final_db_project";

const char* query;

void query1_1(MYSQL* connection, int truck);
void query1_2(MYSQL* connectionm, int truck);
void query1_3(MYSQL* connection, int truck);
void query2(MYSQL* connection);
void query3(MYSQL* connection);
void query4(MYSQL* connection);
void query5(MYSQL* connection);


int main(void) {

	MYSQL* connection = NULL;
	MYSQL conn;

	FILE* fp1 = NULL;
	FILE* fp2 = NULL;
	FILE* fp3 = NULL;

	char strTemp[500];

	if (mysql_init(&conn) == NULL)
		printf("mysql_init() error!");

	connection = mysql_real_connect(&conn, host, user, pw, db, 3306, (const char*)NULL, 0);
	if (connection == NULL){
		printf("%d ERROR : %s\n", mysql_errno(&conn), mysql_error(&conn));
		return 1;
	}
	else{
		printf("Connection Succeed\n");

		if (mysql_select_db(&conn, db)){
			printf("%d ERROR : %s\n", mysql_errno(&conn), mysql_error(&conn));
			return 1;
		}


		fp1 = fopen("20200110_DDL.txt", "r"); //파일 읽어와서 테이블 생성하기!!
		if (fp1 != NULL){

			while (!feof(fp1))
			{
				query = fgets(strTemp, sizeof(strTemp), fp1);
				mysql_query(connection, query); //mysql에 txt의 줄마다 있는 query를 보내 처리
			}
		
		}// txt 파일을 통한 Create 완료
		else
		{
			printf("file open error");
			return 1;
		}// file open error인 경우 처리
		
		fp2 = fopen("20200110_RelationsInsertFile.txt", "r"); // 파일 읽어와서 실제 인스턴스 데이터 넣기!
		if (fp2 != NULL) {

			while (!feof(fp2)){
				query = fgets(strTemp, sizeof(strTemp), fp2);
				mysql_query(connection, query); //mysql에 txt의 줄마다 있는 query를 보내 처리
			}
			
		}// txt 파일을 통한 Insert 완료
		else
		{
			printf("file open error");
			return 1;
		}// file open error인 경우 처리
		

		int opt;
		int truck;

		// 본격적인 프로그램 시작!!
		printf("******************************************************\n");
		printf("************ JTH Package Delivery DataBase ***********\n");

		while (1) {
			printf("------- SELECT QUERY TYPES -------\n\n");
			printf("\t1. TYPE 1\n");
			printf("\t2. TYPE 2\n");
			printf("\t3. TYPE 3\n");
			printf("\t4. TYPE 4\n");
			printf("\t5. TYPE 5\n");
			printf("\t0. QUIT\n");
			//printf("----------------------------------\n");
			scanf("%d", &opt);

			if (opt == 0) {
				break; // 프로그램 종료
			}
			else if(opt == 1) {
				printf("**  Assume truck X is destroyed in a crash. **\n");
				printf("Which truck X? : ");
				scanf("%d", &truck);
				printf("\n");

				printf("----- Subtypes in TYPE I -----\n");
				printf("    1. TYPE I-1. \n");
				printf("    2. TYPE I-2. \n");
				printf("    3. TYPE I-3. \n");
				int sub_opt;
				scanf("%d", &sub_opt);

				if (sub_opt == 1) {
					query1_1(connection, truck);
				}
				else if (sub_opt == 2) {
					query1_2(connection, truck);
				}
				else if (sub_opt == 3) {
					query1_3(connection, truck);
				}
				else { // 1 2 3 이외 나머지 번호 눌렀을 때 다시 Subtypes 화면으로 돌아가도록!
					continue;
				}
			}
			else if (opt == 2) {
				query2(connection);
			}
			else if (opt == 3) {
				query3(connection);
			}
			else if (opt == 4) {
				query4(connection);
			}
			else if (opt == 5) {
				query5(connection);
			}
			else { // 0~5 이외의 번호 눌렀을 때 다시 메인화면으로 돌아가도록 
				continue;
			}

		}

	}

	
	fp3 = fopen("20200110_drop.txt", "r"); // 파일 읽어와서 drop 테이블 하기
	if (fp3 != NULL) {

		while (!feof(fp3)){
			query = fgets(strTemp, sizeof(strTemp), fp3);
			mysql_query(connection, query); //mysql에 txt의 줄마다 있는 query를 보내 처리
		}
	
		
	}// drop완료
	else{
		printf("file open error");
		return 1;
	}// file open error인 경우 처리
	
	
	fclose(fp1);
	fclose(fp2);
	fclose(fp3);

	mysql_close(connection);

	return 0;
}

void query1_1(MYSQL* connection, int truck) {

	char update[10000];
	printf("------- TYPE I-1 -------\n");
	printf("\n");
	printf("Find all customers who had a package on the truck at the time of the crash.\n");

	sprintf(update, "SELECT customer.customer_name FROM truck_delivery INNER JOIN package ON truck_delivery.package_id = package.package_id INNER JOIN customer ON package.customer_id = customer.customer_id WHERE truck_delivery.truck_id = %d AND truck_delivery.truck_delivery_status = 'now'; ", truck);
	mysql_query(connection, update); // 쿼리 보내는 함수

	sql_result = mysql_store_result(connection); // 쿼리 결과를 여기에 저장
	printf("<customer_name>\n\n");
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
		printf("%s \n", sql_row[0]);
	}
	printf("\n\n\n");

}

void query1_2(MYSQL* connection, int truck) {

	char update[10000];
	printf("------- TYPE I-2 -------\n");
	printf("\n");
	printf("Find all recipients who had a package on that truck at the time of the crash.\n");

	sprintf(update, "SELECT customer.customer_name FROM truck_delivery INNER JOIN package ON truck_delivery.package_id = package.package_id INNER JOIN customer ON package.customer_id = customer.customer_id WHERE truck_delivery.truck_id = %d AND truck_delivery.truck_delivery_status = 'now' and package.recipient_id = customer.customer_id; ", truck);
	mysql_query(connection, update); // 쿼리 보내는 함수

	sql_result = mysql_store_result(connection); // 쿼리 결과를 여기에 저장
	printf("<customer_name>\n\n");
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
		printf("%s \n", sql_row[0]);
	}
	printf("\n\n\n");

}

void query1_3(MYSQL* connection, int truck) {
	// 사고 날짜를 2023 06 10 로 가정
	char update[10000];
	printf("------- TYPE I-3 -------\n");
	printf("\n");
	printf("Find the last successful delivery by that truck prior to the crash.\n");

	sprintf(update, "SELECT truck_delivery.truck_delivery_date, package.package_content, customer_name FROM truck_delivery INNER JOIN package ON truck_delivery.package_id = package.package_id INNER JOIN customer ON package.customer_id = customer.customer_id WHERE truck_delivery.truck_id = %d AND truck_delivery.truck_delivery_status = 'later' ORDER BY 20230610 - truck_delivery.truck_delivery_date LIMIT 1; ", truck);
	mysql_query(connection, update); // 쿼리 보내는 함수

	sql_result = mysql_store_result(connection); // 쿼리 결과를 여기에 저장
	printf("<truck_delivery_date> \t   <package_content> \t   <customer_name>\n\n");
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
		printf("     %s                 %s              %s\n", sql_row[0], sql_row[1], sql_row[2]);
	}
	printf("\n\n\n");

}


void query2(MYSQL* connection) {

	int year;
	char update[10000];
	printf("------- TYPE 2 -------\n");
	printf("\n");
	printf("**  Find the customer who has shipped the most packages in the past year. **\n");
	printf("Which Year? : ");
	scanf("%d", &year);
	
	sprintf(update, "SELECT customer.customer_name, COUNT(*) as order_count FROM customer JOIN bill ON customer.customer_id = bill.customer_id WHERE %d00000000 <= bill.bill_id AND bill.bill_id < %d00000000 GROUP BY customer.customer_id ORDER BY order_count DESC LIMIT 1", year, year + 1);
	mysql_query(connection, update); // 쿼리 보내는 함수
	
	sql_result = mysql_store_result(connection); // 쿼리 결과를 여기에 저장
	
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL){
		printf("customer_name : %s \n", sql_row[0]);
	}
	printf("\n\n\n");

}

void query3(MYSQL* connection) {

	int year;
	char update[10000];
	printf("------- TYPE 3 -------\n");
	printf("\n");

	printf("**  Find the customer who has spent the most money on shipping in the past year. **\n");
	printf("Which Year? : ");
	scanf("%d", &year);

	sprintf(update, "SELECT customer.customer_name, SUM(bill.bill_charge) as total_charge FROM bill INNER JOIN customer ON bill.customer_id = customer.customer_id WHERE LEFT(CAST(bill.bill_id AS CHAR), 4) = '%d' GROUP BY customer.customer_name ORDER BY total_charge DESC LIMIT 1;", year);
	mysql_query(connection, update); // 쿼리 보내는 함수

	sql_result = mysql_store_result(connection); // 쿼리 결과를 여기에 저장

	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
		printf("customer_name : %s \n", sql_row[0]);
	}
	printf("\n\n\n");

}

void query4(MYSQL* connection) {

	char update[10000];
	printf("------- TYPE 4 -------\n");
	printf("\n");

	printf("**  Find the packages that were not delivered within the promised time. **\n");

	sprintf(update, "SELECT c.customer_name, p.package_content, p.package_expected_time, p.package_arrived_time FROM customer AS c JOIN package AS p ON c.customer_id = p.customer_id WHERE(p.package_expected_time - p.package_arrived_time) < 0; ");
	mysql_query(connection, update); // 쿼리 보내는 함수

	sql_result = mysql_store_result(connection); // 쿼리 결과를 여기에 저장
	printf("<customer_name> \t <package_content> \t <package_expected_time> \t <package_arrived_time>\n\n");
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
		printf("%-25s\t %-10s\t %15s    \t              %10s\n", sql_row[0], sql_row[1], sql_row[2], sql_row[3]);
	}
	printf("\n\n\n");

}

void query5(MYSQL* connection) {

	char update[10000];
	int year, month;
	int next_year, next_month;
	printf("------- TYPE 5 -------\n");
	printf("\n");

	printf("**  Generate the bill for each customer for the past month. Consider creating several types of bills. **\n");
	printf("Which Year and Month? : ");
	scanf("%d %d", &year, &month);
	
	// 연, 월에 대한 예외처리
	if (month == 12) {
		next_year = year + 1;
		next_month = 1;
	}
	else {
		next_year = year;
		next_month = month + 1;
	}

	if (month < 10 && next_month < 10) {
		sprintf(update, "SELECT customer.customer_name, customer.customer_address, bill.bill_charge, bill.bill_servicetype FROM bill INNER JOIN customer ON bill.customer_id = customer.customer_id WHERE bill.bill_id >= %d0%d000000 AND bill.bill_id < %d0%d000000; ", year, month, next_year, next_month);
	}
	else if (month < 10) {
		sprintf(update, "SELECT customer.customer_name, customer.customer_address, bill.bill_charge, bill.bill_servicetype FROM bill INNER JOIN customer ON bill.customer_id = customer.customer_id WHERE bill.bill_id >= %d0%d000000 AND bill.bill_id < %d%d000000; ", year, month, next_year, next_month);
	}
	else if (next_month < 10) {
		sprintf(update, "SELECT customer.customer_name, customer.customer_address, bill.bill_charge, bill.bill_servicetype FROM bill INNER JOIN customer ON bill.customer_id = customer.customer_id WHERE bill.bill_id >= %d%d000000 AND bill.bill_id < %d0%d000000; ", year, month, next_year, next_month);
	}
	else { // 둘다 10 이상
		sprintf(update, "SELECT customer.customer_name, customer.customer_address, bill.bill_charge, bill.bill_servicetype FROM bill INNER JOIN customer ON bill.customer_id = customer.customer_id WHERE bill.bill_id >= %d%d000000 AND bill.bill_id < %d%d000000; ", year, month, next_year, next_month);
	}

	
	mysql_query(connection, update); // 쿼리 보내는 함수

	sql_result = mysql_store_result(connection); // 쿼리 결과를 여기에 저장

	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) {
		printf("************** BILL ***************\n\n");
		printf("Customer Name: %s\n", sql_row[0]);
		printf("Customer Address: %s\n", sql_row[1]);
		printf("Charge: $%s0\n", sql_row[2]);
		printf("Bill_service_type: %s\n\n", sql_row[3]);

		printf("           Thank you               \n");
		printf("***********************************\n\n");
	}
	printf("\n\n\n");

}


/*
	printf("------- SELECT QUERY TYPES -------\n\n");
	printf("\t1. TYPE 1\n");
	printf("\t2. TYPE 2\n");
	printf("\t3. TYPE 3\n");
	printf("\t4. TYPE 4\n");
	printf("\t5. TYPE 5\n");
	printf("\t6. TYPE 6\n");
	printf("\t7. TYPE 7\n");
	printf("\t0. QUIT\n");
	//printf("----------------------------------\n");
	printf("\n\n");
	printf("---- TYPE 5 ----\n\n");
	printf("** Find the top k brands by unit sales by the year**\n");
	printf(" Which K? : 3\n");

	return 0;
}
*/
