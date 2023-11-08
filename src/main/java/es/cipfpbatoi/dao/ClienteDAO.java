package es.cipfpbatoi.dao;

import es.cipfpbatoi.modelo.Cliente;
import org.checkerframework.checker.units.qual.C;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ClienteDAO implements GenericDAO<Cliente> {

    final String SQLSELECTALL = "SELECT * FROM clientes";
    final String SQLSELECTPK = "SELECT * FROM clientes WHERE id = ?";
    final String SQLINSERT = "INSERT INTO clientes (nombre, direccion) VALUES (?, ?)";
    final String SQLUPDATE = "UPDATE clientes SET nombre = ?, direccion = ? WHERE id = ?";
    final String SQLDELETE = "DELETE FROM clientes WHERE id = ?";
    final String SQLCOUNT = "SELECT COUNT(*) AS total FROM clientes";
    private final PreparedStatement pstSelectPK;
    private final PreparedStatement pstSelectAll;
    private final PreparedStatement pstInsert;
    private final PreparedStatement pstUpdate;
    private final PreparedStatement pstDelete;
    private final PreparedStatement pstCount;
    Connection con;

    public ClienteDAO() throws SQLException {
        con = ConexionBD.getConexion();
        pstSelectPK = con.prepareStatement(SQLSELECTPK);
        pstSelectAll = con.prepareStatement(SQLSELECTALL, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
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

    private Cliente build(int id, String nombre, String direccion) {
        return new Cliente(id, nombre, direccion);
    }

    public Cliente find(int id) throws SQLException {
        Cliente c = null;
        pstSelectPK.setInt(1, id);
        ResultSet rs = pstSelectPK.executeQuery();
        if (rs.next()) {
            c = build(id, rs.getString("nombre"), rs.getString("direccion"));
        }
        rs.close();
        return c;
    }

    public List<Cliente> findAll() throws SQLException {
        List<Cliente> listaClientes = new ArrayList<Cliente>();
        ResultSet rs = pstSelectAll.executeQuery();
        while (rs.next()) {
            listaClientes.add(build(rs.getInt("id"), rs.getString("nombre"), rs.getString("direccion")));
        }
        rs.close();
        return listaClientes;
    }

//	public boolean insert(Cliente cliInsertar) throws SQLException {
//		pstInsert.setString(1, cliInsertar.getNombre());
//		pstInsert.setString(2, cliInsertar.getDireccion());
//		int insertados = pstInsert.executeUpdate();
//		return (insertados == 1);
//	}

    public Cliente insert(Cliente cliInsertar) throws SQLException {
        pstInsert.setString(1, cliInsertar.getNombre());
        pstInsert.setString(2, cliInsertar.getDireccion());
        int insertados = pstInsert.executeUpdate();
        if (insertados == 1) {
            ResultSet rsClave = pstInsert.getGeneratedKeys();
            rsClave.next();
            int idAsignada = rsClave.getInt(1);
            cliInsertar.setId(idAsignada);
            rsClave.close();
            return cliInsertar;
        }
        return null;
    }

    public boolean update(Cliente cliActualizar) throws SQLException {
        pstUpdate.setString(1, cliActualizar.getNombre());
        pstUpdate.setString(2, cliActualizar.getDireccion());
        pstUpdate.setInt(3, cliActualizar.getId());
        int actualizados = pstUpdate.executeUpdate();
        return (actualizados == 1);
    }

    @Override
    public boolean save(Cliente cliente) throws SQLException {
        if (exists(cliente.getId())) {
            return update(cliente);
        } else {
            return !(insert(cliente) == null);
        }
    }

    public boolean delete(int id) throws SQLException {
        pstDelete.setInt(1, id);
        int borrados = pstDelete.executeUpdate();
        return (borrados == 1);
    }

    public boolean delete(Cliente cliEliminar) throws SQLException {
        return this.delete(cliEliminar.getId());
    }

    @Override
    public long size() throws SQLException {
        try (ResultSet rs = pstCount.executeQuery()) {
            if (rs.next()) {
                return rs.getLong("total");
            }
        }
        return 0;
    }

    @Override
    public List<Cliente> findByExample(Cliente cliente) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT * FROM clientes WHERE true");
        List<Object> propiedades = new ArrayList<>();

        if (cliente.getId()!=0){
            sql.append(" AND id = ?");
            propiedades.add(cliente.getId());
        }
        if (cliente.getNombre()!=null && !cliente.getNombre().isEmpty()){
            sql.append(" AND nombre LIKE ?");
            propiedades.add("%" + cliente.getNombre() + "%");
        }
        if (cliente.getDireccion()!=null && !cliente.getDireccion().isEmpty()){
            sql.append(" AND direccion LIKE ?");
            propiedades.add("%" + cliente.getDireccion() + "%");
        }

        PreparedStatement pst = con.prepareStatement(sql.toString());
        int cont = 1;
        for (Object propiedad : propiedades){
            pst.setObject(cont, propiedad);
            cont++;
        }

        ResultSet rs = pst.executeQuery();
        List<Cliente> clientes = new ArrayList<>();
        while(rs.next()){
            clientes.add(new Cliente(
                    rs.getInt("id"),
                    rs.getString("nombre"),
                    rs.getString("direccion")
            ));
        }
        rs.close();
        return clientes;
    }

    public boolean exists(int id) throws SQLException {
        return find(id) != null;
    }
}
