package es.cipfpbatoi.dao;

import es.cipfpbatoi.modelo.LineaFactura;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class LineaFacturaDAO implements GenericDAO<LineaFactura> {
    private static final String SQLSELECTPK = "SELECT * FROM lineas_factura WHERE linea = ? AND factura = ?";
    private static final String SQLINSERT = "INSERT INTO lineas_factura (linea, factura, articulo, cantidad, importe) VALUES (?, ?, ?, ?, ?)";
    private static final String SQLUPDATE = "UPDATE lineas_factura SET cantidad = ?, importe = ? WHERE linea = ? AND factura = ?";
    private static final String SQLDELETE = "DELETE FROM lineas_factura WHERE linea = ? AND factura = ?";
    private static final String SQLBYFACTURA = "SELECT * FROM lineas_factura WHERE factura = ?";

    private final PreparedStatement pstSelectPK;
    private final PreparedStatement pstInsert;
    private final PreparedStatement pstUpdate;
    private final PreparedStatement pstDelete;
    private final PreparedStatement pstByFactura;
    private final FacturaDAO facturaDAO;


    public LineaFacturaDAO() throws SQLException {
        Connection con = ConexionBD.getConexion();
        pstSelectPK = con.prepareStatement(SQLSELECTPK, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        pstInsert = con.prepareStatement(SQLINSERT);
        pstUpdate = con.prepareStatement(SQLUPDATE);
        pstDelete = con.prepareStatement(SQLDELETE);
        pstByFactura = con.prepareStatement(SQLBYFACTURA, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        facturaDAO = new FacturaDAO();
    }

    public void cerrar() throws SQLException {
        pstSelectPK.close();
        pstInsert.close();
        pstUpdate.close();
        pstDelete.close();
        pstByFactura.close();
    }

    private LineaFactura build(int linea, int factura, int articulo, int cantidad, float importe) {
        return new LineaFactura(linea, factura, articulo, cantidad, importe);
    }

    @Override
    public LineaFactura find(int id) throws Exception {
        return null;
    }



    public LineaFactura find(int linea, int factura) throws SQLException {
        LineaFactura lineaFactura = null;
        pstSelectPK.setInt(1, linea);
        pstSelectPK.setInt(2, factura);
        ResultSet rs = pstSelectPK.executeQuery();
        if (rs.next()) {
            lineaFactura = build(linea, factura, rs.getInt("articulo"), rs.getInt("cantidad"), rs.getFloat("importe"));
        }
        return lineaFactura;
    }

    public List<LineaFactura> findByFactura(int factura) throws SQLException {
        List<LineaFactura> lineaFacturaList = new ArrayList<>();
        pstByFactura.setInt(1, factura);
        ResultSet rs = pstByFactura.executeQuery();
        while (rs.next()) {
            lineaFacturaList.add(build(rs.getInt("linea"), factura, rs.getInt("articulo"), rs.getInt("cantidad"), rs.getFloat("importe")));
        }
        return lineaFacturaList;
    }

    @Override
    public List<LineaFactura> findAll() throws Exception {
        return null;
    }

    @Override
    public LineaFactura insert(LineaFactura lineaFactura) throws SQLException {

        int nextLine = facturaDAO.getNextLine(lineaFactura.getFactura());
        lineaFactura.setLinea(nextLine);
        pstInsert.setInt(1, lineaFactura.getLinea());
        pstInsert.setInt(2, lineaFactura.getFactura());
        pstInsert.setInt(3, lineaFactura.getArticulo());
        pstInsert.setInt(4, lineaFactura.getCantidad());
        pstInsert.setFloat(5, lineaFactura.getImporte());
        int insertados = pstInsert.executeUpdate();

        if (insertados == 1) {
            return lineaFactura;
        }
        return null;
    }

    @Override
    public boolean update(LineaFactura lineaFactura) throws SQLException {
        pstUpdate.setInt(1, lineaFactura.getCantidad());
        pstUpdate.setFloat(2, lineaFactura.getImporte());
        pstUpdate.setInt(3, lineaFactura.getLinea());
        pstUpdate.setInt(4, lineaFactura.getFactura());
        int actualizados = pstUpdate.executeUpdate();
        return (actualizados == 1);
    }

    @Override
    public boolean save(LineaFactura lineaFactura) throws SQLException {
        if (find(lineaFactura.getLinea(), lineaFactura.getFactura())!=null) {
            return update(lineaFactura);
        } else {
            return !(insert(lineaFactura) == null);
        }
    }

    @Override
    public boolean delete(int id) throws SQLException {
        return false;
    }

    @Override
    public boolean delete(LineaFactura lineaFactura) throws Exception {
        pstDelete.setInt(1, lineaFactura.getLinea());
        pstDelete.setInt(2, lineaFactura.getFactura());
        int borrados = pstDelete.executeUpdate();
        return (borrados == 1);
    }


    public long size()throws SQLException{
        return -1;
    }

    @Override
    public List<LineaFactura> findByExample(LineaFactura lineaFactura){
        return null;
    }

    public boolean exists(int linea, int factura) throws SQLException {
        return find(linea, factura) != null;
    }

}
