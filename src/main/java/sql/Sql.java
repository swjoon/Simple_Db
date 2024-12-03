package sql;

public interface Sql {

    SqlImpl append(String sql);

    int insert();
    int update();
    int delete();

}
