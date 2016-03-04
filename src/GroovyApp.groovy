/**
 * Created by C5023792 on 3/4/2016.
 */
import groovy.sql.Sql;

class GroovyApp {
    void hello() {
        println("hello ");
    }

    void insertProviderAttendanceTable(providerId, serviceMonth) {
        def db = Sql.newInstance(
                "jdbc:oracle:thin:@10.237.89.159:1521:tj11gdb8",
                "ECCLA_REBID_30D_T7674_0215",
                "ECCLA_REBID_30D_T7674_0215",
                "oracle.jdbc.driver.OracleDriver");
        println("执行查询语句");
        def sysdate = db.firstRow("select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') from dual")
        println sysdate[0];
        def querySql = "SELECT benefit_id FROM benefit WHERE provider_id = $providerId ORDER BY benefit_id";
        def count = 0;
        def inserSql = "";
//        db.connection.autoCommit = false;
        def orders = db.query(querySql, {
            while (it.next()) {
                def benefitId = it.getString("benefit_id");
                println "benefit_id is " + benefitId;
                count++;
                println("执行修改类语句");
                if (count % 2 == 0) {
                    inserSql = "INSERT INTO provider_attendance_report VALUES (?,?,'MA,DP,AP,DP,AP,AP,MA,DP,AP,MA,DP,IA,DP,AP,IA,DP,AP,MA,DP,IA,DP,MA,DP,AP,IA,DP,IA,MA,DP,IA,NA','0', SYSDATE,SYSDATE)";
                } else if (count % 3 == 0) {
                    inserSql = "INSERT INTO provider_attendance_report VALUES (?,?,'IA,MA,DP,AP,DP,AP,AP,MA,DP,AP,MA,DP,IA,DP,AP,IA,DP,AP,MA,DP,IA,DP,MA,DP,AP,IA,DP,IA,MA,DP,NA','0', SYSDATE,SYSDATE)";
                } else if (count % 4 == 0) {
                    inserSql = "INSERT INTO provider_attendance_report VALUES (?,?,'DP,IA,MA,DP,AP,DP,AP,AP,MA,DP,AP,MA,DP,IA,DP,AP,IA,DP,AP,MA,DP,IA,DP,MA,DP,AP,IA,DP,IA,MA,NA','0', SYSDATE,SYSDATE)";
                } else if (count % 5 == 0) {
                    inserSql = "INSERT INTO provider_attendance_report VALUES (?,?,'MA,DP,IA,MA,DP,AP,DP,AP,AP,MA,DP,AP,MA,DP,IA,DP,AP,IA,DP,AP,MA,DP,IA,DP,MA,DP,AP,IA,DP,IA,NA','0', SYSDATE,SYSDATE)";
                } else if (count % 6 == 0) {
                    inserSql = "INSERT INTO provider_attendance_report VALUES (?,?,'IA,MA,DP,IA,MA,DP,AP,DP,AP,AP,MA,DP,AP,MA,DP,IA,DP,AP,IA,DP,AP,MA,DP,IA,DP,MA,DP,AP,IA,DP,NA','0', SYSDATE,SYSDATE)";
                } else if (count % 7 == 0) {
                    inserSql = "INSERT INTO provider_attendance_report VALUES (?,?,'DP,IA,MA,DP,IA,MA,DP,AP,DP,AP,AP,MA,DP,AP,MA,DP,IA,DP,AP,IA,DP,AP,MA,DP,IA,DP,MA,DP,AP,IA,NA','0', SYSDATE,SYSDATE)";
                } else if (count % 8 == 0) {
                    inserSql = "INSERT INTO provider_attendance_report VALUES (?,?,'IA,DP,IA,MA,DP,IA,MA,DP,AP,DP,AP,AP,MA,DP,AP,MA,DP,IA,DP,AP,IA,DP,AP,MA,DP,IA,DP,MA,DP,AP,NA','0', SYSDATE,SYSDATE)";
                } else if (count % 9 == 0) {
                    inserSql = "INSERT INTO provider_attendance_report VALUES (?,?,'AP,IA,DP,IA,MA,DP,IA,MA,DP,AP,DP,AP,AP,MA,DP,AP,MA,DP,IA,DP,AP,IA,DP,AP,MA,DP,IA,DP,MA,DP,NA','0', SYSDATE,SYSDATE)";
                } else if (count % 10 == 0) {
                    inserSql = "INSERT INTO provider_attendance_report VALUES (?,?,'DP,AP,IA,DP,IA,MA,DP,IA,MA,DP,AP,DP,AP,AP,MA,DP,AP,MA,DP,IA,DP,AP,IA,DP,AP,MA,DP,IA,DP,MA,NA','0', SYSDATE,SYSDATE)";
                } else {
                    inserSql = "INSERT INTO provider_attendance_report VALUES (?,?,'MA,DP,AP,IA,DP,IA,MA,DP,IA,MA,DP,AP,DP,AP,AP,MA,DP,AP,MA,DP,IA,DP,AP,IA,DP,AP,MA,DP,IA,DP,NA','0', SYSDATE,SYSDATE)";
                }
                db.execute(inserSql,[benefitId,serviceMonth]);
                if (count % 600 == 0) {
                    db.commit();
                }
            }
        })
        println "total count is " + count;
        db.commit();
        db.close();
    }
}
