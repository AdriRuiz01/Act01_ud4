package es.cipfpbatoi.dao;

import es.cipfpbatoi.modelo.Factura;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class FacturaDAO implements GenericDAO<Factura>{

    private static final String SQLSELECTALL = "SELECT * FROM facturas";
    private static final String SQLSELECTPK = "SELECT * FROM facturas WHERE id = ?";
    private static final String SQLINSERT = "INSERT INTO facturas (fecha, cliente, vendedor, formapago) VALUES (?, ?, ?, ?)";
    private static final String SQLUPDATE = "UPDATE facturas SET fecha = ?, cliente = ?, vendedor = ?, formapago = ? WHERE id = ?";
    private static final String SQLDELETE = "DELETE FROM facturas WHERE id = ?";
    private static final String SQLCOUNT = "SELECT COUNT(*) AS total FROM facturas";
    private static final String SQLVENDEDOR = "SELECT * FROM facturas WHERE vendedor = ?";
    private static final String SQLCLIENTE = "SELECT * FROM facturas WHERE cliente = ?";
    private static final String SQLNEXTLINE = "SELECT MAX(linea) AS Next FROM lineas_factura WHERE factura = ?";
    private static final String SQLIMPORTETOTAL = "SELECT SUM(importe) AS total FROM lineas_factura WHERE factura = ?";

    private final PreparedStatement pstSelectPK;
    private final PreparedStatement pstSelectAll;
    private final PreparedStatement pstInsert;
    private final PreparedStatement pstUpdate;
    private final PreparedStatement pstDelete;
    private final PreparedStatement pstCount;
    private final PreparedStatement pstVendedor;
    private final PreparedStatement pstCliente;
    private final PreparedStatement pstNextLine;
    private final PreparedStatement pstImporteTotal;


    public FacturaDAO() throws SQLException {
        Connection con = ConexionBD.getConexion();
        pstSelectPK = con.prepareStatement(SQLSELECTPK, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        pstSelectAll = con.prepareStatement(SQLSELECTALL, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        pstInsert = con.prepareStatement(SQLINSERT, PreparedStatement.RETURN_GENERATED_KEYS);
        pstUpdate = con.prepareStatement(SQLUPDATE);
        pstDelete = con.prepareStatement(SQLDELETE);
        pstCount = con.prepareStatement(SQLCOUNT);
        pstVendedor = con.prepareStatement(SQLVENDEDOR, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        pstCliente = con.prepareStatement(SQLCLIENTE, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        pstNextLine = con.prepareStatement(SQLNEXTLINE);
        pstImporteTotal = con.prepareStatement(SQLIMPORTETOTAL);
    }

    public void cerrar() throws SQLException {
        pstSelectPK.close();
        pstSelectAll.close();
        pstInsert.close();
        pstUpdate.close();
        pstDelete.close();
        pstCount.close();
        pstVendedor.close();
        pstCliente.close();
        pstNextLine.close();
    }

    private Factura build(int id, LocalDate fecha, int cliente, int vendedor, String formaPago) {
        return new Factura(id, fecha, cliente, vendedor, formaPago);
    }

    public int getNextLine(int factura) throws SQLException {
        if (exists(factura)){
            pstNextLine.setInt(1,factura);
            ResultSet rs = pstNextLine.executeQuery();
            if (rs.next()){
                return rs.getInt(1)+1;
            }
        }
        return -1;
    }

    @Override
    public Factura find(int id) throws SQLException {
        Factura factura = null;
        pstSelectPK.setInt(1, id);
        ResultSet rs = pstSelectPK.executeQuery();
        if (rs.next()) {
            factura = build(id, rs.getDate("fecha").toLocalDate(), rs.getInt("cliente"), rs.getInt("vendedor"), rs.getString("formapago"));
        }
        return factura;
    }

    @Override
    public List<Factura> findAll() throws SQLException {
        List<Factura> facturaList = new ArrayList<Factura>();
        ResultSet rs = pstSelectAll.executeQuery();
        while (rs.next()) {
            facturaList.add(build(rs.getInt("id"), rs.getDate("fecha").toLocalDate(), rs.getInt("cliente"), rs.getInt("vendedor"), rs.getString("formapago")));
        }
        return facturaList;
    }

    public List<Factura> findByCliente(int cliente) throws SQLException {
        List<Factura> facturaList = new ArrayList<Factura>();
        pstCliente.setInt(1,cliente);
        ResultSet rs = pstCliente.executeQuery();
        while (rs.next()) {
            facturaList.add(build(rs.getInt("id"), rs.getDate("fecha").toLocalDate(), rs.getInt("cliente"), rs.getInt("vendedor"), rs.getString("formapago")));
        }
        return facturaList;
    }
    public List<Factura> findByVendedor(int vendedor) throws SQLException {
        List<Factura> facturaList = new ArrayList<Factura>();
        pstVendedor.setInt(1,vendedor);
        ResultSet rs = pstVendedor.executeQuery();
        while (rs.next()) {
            facturaList.add(build(rs.getInt("id"), rs.getDate("fecha").toLocalDate(), rs.getInt("cliente"), rs.getInt("vendedor"), rs.getString("formapago")));
        }
        return facturaList;
    }

    @Override
    public Factura insert(Factura factura) throws SQLException {
        pstInsert.setDate(1, Date.valueOf(factura.getFecha()));
        pstInsert.setInt(2, factura.getCliente());
        pstInsert.setInt(3, factura.getVendedor());
        pstInsert.setString(4, factura.getFormaPago());
        int insertados = pstInsert.executeUpdate();
        if (insertados == 1) {
            ResultSet rsClave = pstInsert.getGeneratedKeys();
            rsClave.next();
            int idAsignada = rsClave.getInt(1);
            factura.setId(idAsignada);
            return factura;
        }
        return null;
    }

    @Override
    public boolean update(Factura factura) throws SQLException {
        pstUpdate.setDate(1, Date.valueOf(factura.getFecha()));
        pstUpdate.setInt(2, factura.getCliente());
        pstUpdate.setInt(3, factura.getVendedor());
        pstUpdate.setString(4, factura.getFormaPago());
        pstUpdate.setInt(5, factura.getId());
        int actualizados = pstUpdate.executeUpdate();
        return (actualizados == 1);
    }

    @Override
    public boolean save(Factura factura) throws SQLException {
        if (exists(factura.getId())) {
            return update(factura);
        } else {
            return !(insert(factura) == null);
        }
    }

    @Override
    public boolean delete(int id) throws SQLException {
        pstDelete.setInt(1, id);
        int borrados = pstDelete.executeUpdate();
        return (borrados == 1);
    }

    @Override
    public boolean delete(Factura factura) throws SQLException {
        return delete(factura.getId());
    }

    @Override
    public long size() throws SQLException {
       try(ResultSet rs = pstCount.executeQuery()) {
           if (rs.next()){
               return rs.getLong("total");
           }
       }
       return 0;
    }

    @Override
    public List<Factura> findByExample(Factura factura){
        return null;
    }

    public boolean exists(int id) throws SQLException {
        return find(id) != null;
    }

    public double getImporteTotal(int id)throws SQLException {
        if (exists(id)){
            pstImporteTotal.setInt(1, id);
            ResultSet rs = pstImporteTotal.executeQuery();
            if (rs.next()){
                return rs.getFloat("total");
            }
        }
        return -1;
    }
}
