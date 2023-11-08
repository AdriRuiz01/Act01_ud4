package es.cipfpbatoi.dao;

import es.cipfpbatoi.modelo.Vendedor;
import org.checkerframework.checker.units.qual.A;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class VendedorDAO implements GenericDAO<Vendedor>{

    final String SQLSELECTALL = "SELECT * FROM vendedores";
    final String SQLSELECTPK = "SELECT * FROM vendedores WHERE id = ?";
    final String SQLINSERT = "INSERT INTO vendedores (nombre, fecha_ingreso, salario) VALUES (?, ?, ?)";
    final String SQLUPDATE = "UPDATE vendedores SET nombre = ?, fecha_ingreso = ?, salario = ? WHERE id = ?";
    final String SQLDELETE = "DELETE FROM vendedores WHERE id = ?";
    final String SQLCOUNT = "SELECT COUNT(*) AS total FROM vendedores";

    private final PreparedStatement pstSelectPK;
    private final PreparedStatement pstSelectAll;
    private final PreparedStatement pstInsert;
    private final PreparedStatement pstUpdate;
    private final PreparedStatement pstDelete;
    private final PreparedStatement pstCount;
    Connection con;

    public VendedorDAO() throws SQLException {
        con = ConexionBD.getConexion();
        pstSelectPK = con.prepareStatement(SQLSELECTPK);
        pstSelectAll = con.prepareStatement(SQLSELECTALL,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        pstInsert = con.prepareStatement(SQLINSERT, PreparedStatement.RETURN_GENERATED_KEYS);
        pstUpdate = con.prepareStatement(SQLUPDATE);
        pstDelete = con.prepareStatement(SQLDELETE);
        pstCount = con.prepareStatement(SQLCOUNT);
    }

    public void cerrar() throws SQLException {
        pstSelectPK.close();
        pstSelectAll.close();
        pstInsert.close();
        pstUpdate.close();
        pstDelete.close();
        pstCount.close();
    }

    private Vendedor build(int id, String nombre, LocalDate fechaIngreso, float salario) {
        return new Vendedor(id, nombre, fechaIngreso, salario);
    }





    @Override
    public Vendedor find(int id) throws SQLException {
        Vendedor v = null;
        pstSelectPK.setInt(1, id);
        ResultSet rs = pstSelectPK.executeQuery();
        if (rs.next()) {
            v = build(id, rs.getString("nombre"), rs.getDate("fecha_ingreso").toLocalDate(), rs.getFloat("salario"));
        }
        return v;
    }

    @Override
    public List<Vendedor> findAll() throws SQLException {
        List<Vendedor> vendedorList = new ArrayList<Vendedor>();
        ResultSet rs = pstSelectAll.executeQuery();
        while (rs.next()) {
            vendedorList.add(build(rs.getInt("id"), rs.getString("nombre"), rs.getDate("fecha_ingreso").toLocalDate(), rs.getFloat("salario")));
        }
        return vendedorList;
    }

    @Override
    public Vendedor insert(Vendedor vendedor) throws SQLException {
        pstInsert.setString(1, vendedor.getNombre());
        pstInsert.setDate(2, java.sql.Date.valueOf(vendedor.getFechaIngreso()));
        pstInsert.setFloat(3, vendedor.getSalario());

        int insertados = pstInsert.executeUpdate();
        if (insertados == 1) {
            ResultSet rsClave = pstInsert.getGeneratedKeys();
            rsClave.next();
            int idAsignada = rsClave.getInt(1);
            vendedor.setId(idAsignada);
            return vendedor;
        }
        return null;
    }

    @Override
    public boolean update(Vendedor vendedor) throws SQLException {
        pstUpdate.setString(1, vendedor.getNombre());
        pstUpdate.setDate(2, java.sql.Date.valueOf(vendedor.getFechaIngreso()));
        pstUpdate.setFloat(3, vendedor.getSalario());
        pstUpdate.setInt(4, vendedor.getId());

        int actualizados = pstUpdate.executeUpdate();
        return (actualizados == 1);
    }

    @Override
    public boolean save(Vendedor vendedor) throws SQLException {
        if (exists(vendedor.getId())) {
            return update(vendedor);
        } else {
            return !(insert(vendedor) == null);
        }
    }

    @Override
    public boolean delete(int id) throws SQLException {
        pstDelete.setInt(1, id);
        int borrados = pstDelete.executeUpdate();
        return (borrados == 1);
    }

    @Override
    public boolean delete(Vendedor vendedor) throws SQLException {
        return this.delete(vendedor.getId());
    }

    @Override
    public long size() throws SQLException {
      ResultSet rs = pstCount.executeQuery();
            if (rs.next()){
                return rs.getLong("total");
            }

        return 0;
    }

    @Override
    public List<Vendedor> findByExample(Vendedor vendedor) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT * FROM vendedores WHERE true");
        List<Object> propiedades = new ArrayList<>();

        if (vendedor.getId() != 0) {
            sql.append(" AND id = ?");
            propiedades.add(vendedor.getId());
        }
        if (vendedor.getNombre() != null && !vendedor.getNombre().isEmpty()) {
            sql.append(" AND nombre LIKE ?");
            propiedades.add("%" + vendedor.getNombre() + "%");
        }
        if (vendedor.getFechaIngreso() != null) {
            sql.append(" AND fecha_ingreso <= ?");
            propiedades.add(vendedor.getFechaIngreso());
        }
        if (vendedor.getSalario() != 0) {
            sql.append(" AND salario <= ?");
            propiedades.add(vendedor.getSalario());
        }

        PreparedStatement pst = con.prepareStatement(sql.toString());
        int cont = 1;
             for (Object propiedad : propiedades){
                 pst.setObject(cont, propiedad);
                 cont++;
             }

        ResultSet rs = pst.executeQuery();
        List<Vendedor> vendedores = new ArrayList<>();
        while (rs.next()) {
            vendedores.add(build(
                    rs.getInt("id"),
                    rs.getString("nombre"),
                    rs.getDate("fecha_ingreso").toLocalDate(),
                    rs.getFloat("salario")));
        }
        rs.close();
        return vendedores;
    }

    public boolean exists(int id) throws SQLException {
        return find(id) != null;
    }
}
