package es.cipfpbatoi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import es.cipfpbatoi.dao.ConexionBD;
import es.cipfpbatoi.dao.FacturaDAO;
import es.cipfpbatoi.dao.FacturaDAOPagination;
import es.cipfpbatoi.modelo.Cliente;
import es.cipfpbatoi.modelo.Factura;
import es.cipfpbatoi.modelo.Vendedor;

@TestMethodOrder(OrderAnnotation.class)
class TestFacturaDAOPagination {
	static FacturaDAOPagination capaDao;
	Factura registroVacio = new Factura();
	Cliente cli2 = new Cliente(2, "Diana Perez", "C/ Brito del Pino, 1120");
	Vendedor ven2 = new Vendedor(2, "Juan Fernandez", LocalDate.of(2014, 3, 1), 11500f);

	Factura registroExiste1 = new Factura(2, LocalDate.of(2008, 3, 18), 2, 2, "Contado");
	Factura registroExiste2 = new Factura(17, LocalDate.of(2012, 10, 11), 2, 2, "transferencia");
	Factura registroNoExiste = new Factura(10000, LocalDate.of(0, 1, 1), 2, 2, "Contado");

	Factura registroNuevo = new Factura(LocalDate.of(2021, 12, 1), 2, 2, "tarjeta");
	Factura registroNuevoError = new Factura(LocalDate.of(2021, 12, 1), 100, 2, "tarjeta");
	Factura registroModificarBorrar = new Factura(5000, LocalDate.of(2021, 12, 2), 2, 2, "Contado");
	Factura registroModificarBorrarError = new Factura(5000, LocalDate.of(2021, 12, 2), 100, 2, "Contado");

	Factura registroPrimeroPag1Tam100 = new Factura(1, null, 0, 0, null);
	Factura registroUltimoPag1Tam100 = new Factura(100, null, 0, 0, null);
	Factura registroPrimeroPag5Tam100 = new Factura(401, null, 0, 0, null);
	Factura registroUltimoPag5Tam100 = new Factura(500, null, 0, 0, null);
	Factura registroPrimeroPag50Tam100 = new Factura(4901, null, 0, 0, null);
	Factura registroUltimoPag50Tam100 = new Factura(5000, null, 0, 0, null);
	
	Factura registroPrimeroPag1Tam75 = new Factura(1, null, 0, 0, null);
	Factura registroUltimoPag1Tam75 = new Factura(75, null, 0, 0, null);
	Factura registroPrimeroPag5Tam75 = new Factura(301, null, 0, 0, null);
	Factura registroUltimoPag5Tam75 = new Factura(375, null, 0, 0, null);
	Factura registroPrimeroPag50Tam75 = new Factura(3676, null, 0, 0, null);
	Factura registroUltimoPag50Tam75 = new Factura(3750, null, 0, 0, null);
	Factura registroPrimeroPag67Tam75 = new Factura(4951, null, 0, 0, null);
	Factura registroUltimoPag67Tam75 = new Factura(5000, null, 0, 0, null);
	
	
	Factura registroPrimeroPag1Cli2Tam100 = new Factura(2, null, 0, 0, null);
	Factura registroUltimoPag1Cli2Tam100 = new Factura(291, null, 0, 0, null);
	Factura registroPrimeroPag5Cli2Tam100 = new Factura(1154, null, 0, 0, null);
	Factura registroUltimoPag5Cli2Tam100= new Factura(1474, null, 0, 0, null);
	
	Factura registroPrimeroPag1Cli2Tam75 = new Factura(2, null, 0, 0, null);
	Factura registroUltimoPag1Cli2Tam75 = new Factura(217, null, 0, 0, null);
	Factura registroPrimeroPag5Cli2Tam75 = new Factura(838, null, 0, 0, null);
	Factura registroUltimoPag5Cli2Tam75 = new Factura(1091, null, 0, 0, null);
	
	
	Factura registroPrimeroPag1Vend2Tam100 = new Factura(2, null, 0, 0, null);
	Factura registroUltimoPag1Vend2Tam100 = new Factura(195, null, 0, 0, null);	
	Factura registroPrimeroPag5Vend2Tam100 = new Factura(789, null, 0, 0, null);
	Factura registroUltimoPag5Vend2Tam100 = new Factura(987, null, 0, 0, null);
	
	Factura registroPrimeroPag1Vend2Tam75 = new Factura(2, null, 0, 0, null);
	Factura registroUltimoPag1Vend2Tam75 = new Factura(148, null, 0, 0, null);	
	Factura registroPrimeroPag5Vend2Tam75 = new Factura(587, null, 0, 0, null);
	Factura registroUltimoPag5Vend2Tam75 = new Factura(741, null, 0, 0, null);
		
	static int numRegistrosEsperado = 5000;
	static int autoIncrement = 5000;
	final static String TABLA = "facturas";
	final static String BD = "empresa_ad_test";
	final int PAGESIZE100 = 100;
	final int PAGESIZE75 = 75;

	@BeforeAll
	static void setUpBeforeClass() {
		try {
			capaDao = new FacturaDAOPagination();

			ConexionBD.getConexion().createStatement()
					.executeUpdate("delete from " + BD + "." + TABLA + " where id >= " + numRegistrosEsperado);

			if (ConexionBD.getConexion().getMetaData().getDatabaseProductName().equals("MariaDB")) {
				ConexionBD.getConexion().createStatement()
						.executeUpdate("ALTER TABLE " + BD + "." + TABLA + " AUTO_INCREMENT = " + autoIncrement);
			} else { // PostgreSQL
				ConexionBD.getConexion().createStatement()
						.executeUpdate("ALTER SEQUENCE " + BD + "." + TABLA + "_id_seq RESTART WITH " + autoIncrement);
			}

			ConexionBD.getConexion().createStatement().executeUpdate("insert into " + BD + "." + TABLA
					+ "(fecha, cliente, vendedor, formapago) values ('2021-11-30', 4, 1, 'Contado')");

		} catch (SQLException e) {
			fail("El test falla al preparar el test (instanciando dao: posiblemente falla la conexión a la BD)"
					+ e.getMessage());
		}
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		capaDao.cerrar();
	}

//	@BeforeEach
//	static void setUp() {
//		try {
//			ConexionBD.getConexion().createStatement().executeUpdate("delete from empresa_ad.clientes where id > 5");
//		} catch (SQLException e) {
//			fail("El test falla en la preparación antes de cada test (preparando tabla clientes)");
//		}
//	}

	@Test
	@Order(1)
	void testfind() {
		Factura registroObtenido;
		try {
			registroObtenido = capaDao.find(registroExiste1.getId());
			Factura registroEsperado = registroExiste1;
			assertEquals(registroEsperado, registroObtenido);
			registroObtenido = capaDao.find(registroExiste2.getId());
			registroEsperado = registroExiste2;
			assertEquals(registroEsperado, registroObtenido);
			registroObtenido = capaDao.find(registroNoExiste.getId());
			assertNull(registroObtenido);
		} catch (SQLException e) {
			fail("El testfind falla" + e.getMessage());
		}
	}

	@Test
	@Order(1)
	void testFindAll() {
		int numRegistrosObtenido;
		try {
			capaDao.setPageSize(PAGESIZE100);
			List<Factura> lista = capaDao.findAll();
			numRegistrosObtenido = lista.size();
			assertEquals(PAGESIZE100, numRegistrosObtenido);
			assertEquals(registroPrimeroPag1Tam100.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag1Tam100.getId(), lista.get(lista.size() - 1).getId());
			
			capaDao.setPageSize(PAGESIZE75);
			lista = capaDao.findAll();
			numRegistrosObtenido = lista.size();
			assertEquals(PAGESIZE75, numRegistrosObtenido);
			assertEquals(registroPrimeroPag1Tam75.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag1Tam75.getId(), lista.get(lista.size() - 1).getId());
		} catch (SQLException e) {
			fail("El testfindall falla" + e.getMessage());
		}
	}

	@Test
	@Order(1)
	void testFindAllPage5() {
		int numRegistrosObtenido;
		try {
			capaDao.setPageSize(PAGESIZE100);
			List<Factura> lista = capaDao.findAll(5);
			numRegistrosObtenido = lista.size();
			assertEquals(PAGESIZE100, numRegistrosObtenido);
			assertEquals(registroPrimeroPag5Tam100.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag5Tam100.getId(), lista.get(lista.size() - 1).getId());
			
			capaDao.setPageSize(PAGESIZE75);
			lista = capaDao.findAll(5);
			numRegistrosObtenido = lista.size();
			assertEquals(PAGESIZE75, numRegistrosObtenido);
			assertEquals(registroPrimeroPag5Tam75.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag5Tam75.getId(), lista.get(lista.size() - 1).getId());
		} catch (SQLException e) {
			fail("El testfindall page5 falla" + e.getMessage());
		}
	}
	
	@Test
	@Order(1)
	void testFindAllPage50() {
		int numRegistrosObtenido;
		try {
			capaDao.setPageSize(PAGESIZE100);
			List<Factura> lista = capaDao.findAll(50);
			numRegistrosObtenido = lista.size();
			assertEquals(PAGESIZE100, numRegistrosObtenido);
			assertEquals(registroPrimeroPag50Tam100.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag50Tam100.getId(), lista.get(lista.size() - 1).getId());
			
			capaDao.setPageSize(PAGESIZE75);
			lista = capaDao.findAll(50);
			numRegistrosObtenido = lista.size();
			assertEquals(PAGESIZE75, numRegistrosObtenido);
			assertEquals(registroPrimeroPag50Tam75.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag50Tam75.getId(), lista.get(lista.size() - 1).getId());
		} catch (SQLException e) {
			fail("El testfindall page50 falla" + e.getMessage());
		}
	}
	
	@Test
	@Order(1)
	void testFindAllPage51() {
		int numRegistrosObtenido;
		try {
			capaDao.setPageSize(PAGESIZE100);
			List<Factura> lista = capaDao.findAll(51);
			numRegistrosObtenido = lista.size();
			assertEquals(0, numRegistrosObtenido);
		} catch (SQLException e) {
			fail("El testfindall page51 falla" + e.getMessage());
		}
	}
	
	@Test
	@Order(1)
	void testFindAllPage67() {
		int numRegistrosObtenido;
		try {
			capaDao.setPageSize(PAGESIZE75);
			List<Factura> lista = capaDao.findAll(67);
			numRegistrosObtenido = lista.size();
			assertEquals(50, numRegistrosObtenido);
			assertEquals(registroPrimeroPag67Tam75.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag67Tam75.getId(), lista.get(lista.size() - 1).getId());
		} catch (SQLException e) {
			fail("El testfindall page 67 falla" + e.getMessage());
		}
	}
	
	@Test
	@Order(1)
	void testFindByCliente() {
		int numRegistrosObtenido;
		try {
			capaDao.setPageSize(PAGESIZE100);
			List<Factura> lista = capaDao.findByCliente(2);
			numRegistrosObtenido = lista.size();
			assertEquals(PAGESIZE100, numRegistrosObtenido);
			assertEquals(registroPrimeroPag1Cli2Tam100.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag1Cli2Tam100.getId(), lista.get(lista.size() - 1).getId());
			
			capaDao.setPageSize(PAGESIZE75);
			lista = capaDao.findByCliente(2);
			numRegistrosObtenido = lista.size();
			assertEquals(PAGESIZE75, numRegistrosObtenido);
			assertEquals(registroPrimeroPag1Cli2Tam75.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag1Cli2Tam75.getId(), lista.get(lista.size() - 1).getId());
		} catch (SQLException e) {
			fail("El testfindcliente falla" + e.getMessage());
		}
	}
	
	@Test
	@Order(1)
	void testFindByClientePage5() {
		int numRegistrosObtenido;
		try {
			capaDao.setPageSize(PAGESIZE100);
			List<Factura> lista = capaDao.findByCliente(2, 5);
			numRegistrosObtenido = lista.size();
			assertEquals(PAGESIZE100, numRegistrosObtenido);
			assertEquals(registroPrimeroPag5Cli2Tam100.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag5Cli2Tam100.getId(), lista.get(lista.size() - 1).getId());
			
			capaDao.setPageSize(PAGESIZE75);
			lista = capaDao.findByCliente(2, 5);
			numRegistrosObtenido = lista.size();
			assertEquals(PAGESIZE75, numRegistrosObtenido);
			assertEquals(registroPrimeroPag5Cli2Tam75.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag5Cli2Tam75.getId(), lista.get(lista.size() - 1).getId());
		} catch (SQLException e) {
			fail("El testfindcliente page5 falla" + e.getMessage());
		}
	}

	@Test
	@Order(1)
	void testFindByVendedor() {
		int numRegistrosObtenido;
		try {
			capaDao.setPageSize(PAGESIZE100);
			List<Factura> lista = capaDao.findByVendedor(2);
			numRegistrosObtenido = lista.size();
			assertEquals(PAGESIZE100, numRegistrosObtenido);
			assertEquals(registroPrimeroPag1Vend2Tam100.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag1Vend2Tam100.getId(), lista.get(lista.size() - 1).getId());
			
			capaDao.setPageSize(PAGESIZE75);
			lista = capaDao.findByVendedor(2);
			numRegistrosObtenido = lista.size();
			assertEquals(PAGESIZE75, numRegistrosObtenido);
			assertEquals(registroPrimeroPag1Vend2Tam75.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag1Vend2Tam75.getId(), lista.get(lista.size() - 1).getId());
		} catch (SQLException e) {
			fail("El testfindvendedor falla" + e.getMessage());
		}
	}
	
	@Test
	@Order(1)
	void testFindByVendedorPage5() {
		int numRegistrosObtenido;
		try {
			capaDao.setPageSize(PAGESIZE100);
			List<Factura> lista = capaDao.findByVendedor(2, 5);
			numRegistrosObtenido = lista.size();
			assertEquals(PAGESIZE100, numRegistrosObtenido);
			assertEquals(registroPrimeroPag5Vend2Tam100.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag5Vend2Tam100.getId(), lista.get(lista.size() - 1).getId());
			
			capaDao.setPageSize(PAGESIZE75);
			lista = capaDao.findByVendedor(2, 5);
			numRegistrosObtenido = lista.size();
			assertEquals(PAGESIZE75, numRegistrosObtenido);
			assertEquals(registroPrimeroPag5Vend2Tam75.getId(), lista.get(0).getId());
			assertEquals(registroUltimoPag5Vend2Tam75.getId(), lista.get(lista.size() - 1).getId());
		} catch (SQLException e) {
			fail("El testfindvendedor page5 falla" + e.getMessage());
		}
	}
	
	@Test
	@Order(1)
	void testGetNumPages() {
		
		try {
			capaDao.setPageSize(PAGESIZE100);
			assertEquals(50, capaDao.getNumPages());
			capaDao.setPageSize(PAGESIZE75);
			assertEquals(67, capaDao.getNumPages());
			capaDao.setPageSize(35);
			assertEquals(143, capaDao.getNumPages());
			
		} catch (SQLException e) {
			fail("El testgetnumpages falla" + e.getMessage());
		}
		
	}

	@Test
	@Order(2)
	void testInsert() {
		try {
			Factura registro = capaDao.insert(registroNuevo);
			assertNotNull(registro);
			assertNotEquals(0, registro.getId());
			assertEquals(numRegistrosEsperado + 1, registro.getId());
		} catch (SQLException e) {
			fail("El testinsert falla" + e.getMessage());
		}

		Exception ex = assertThrows(SQLException.class, () -> {
			capaDao.insert(registroNuevoError);
		});
		assertTrue(!ex.getMessage().isEmpty());
	}

	@Test
	@Order(3)
	void testUpdate() {
		boolean respuestaObtenida;
		try {
			respuestaObtenida = capaDao.update(registroModificarBorrar);
			assertTrue(respuestaObtenida);
			respuestaObtenida = capaDao.update(registroNoExiste);
			assertFalse(respuestaObtenida);
		} catch (SQLException e) {
			fail("El testupdate falla" + e.getMessage());
		}

		Exception ex = assertThrows(SQLException.class, () -> {
			capaDao.update(registroModificarBorrarError);
		});
		assertTrue(!ex.getMessage().isEmpty());
	}

	@Test
	@Order(4)
	void testSave() {
		Boolean respuestaObtenida;
		try {
			respuestaObtenida = capaDao.save(registroModificarBorrar);
			assertTrue(respuestaObtenida);
			respuestaObtenida = capaDao.save(registroNoExiste);
			assertTrue(respuestaObtenida);
			assertNotEquals(0, registroNoExiste.getId());
			assertEquals(numRegistrosEsperado + 3, registroNoExiste.getId());
		} catch (SQLException e) {
			fail("Falla testSave" + e.getMessage());
		}

		Exception ex = assertThrows(SQLException.class, () -> {
			capaDao.save(registroModificarBorrarError);
		});
		assertTrue(!ex.getMessage().isEmpty());
	}

	@Test
	@Order(5)
	void testDelete() {
		boolean respuestaObtenida;
		try {
			respuestaObtenida = capaDao.delete(registroModificarBorrar);
			assertTrue(respuestaObtenida);
			respuestaObtenida = capaDao.delete(registroNoExiste);
			assertFalse(respuestaObtenida);
		} catch (SQLException e) {
			fail("El testdelete falla" + e.getMessage());
		}
	}

	@Test
	@Order(1)
	void testSize() {
		long respuestaObtenida;
		try {
			respuestaObtenida = capaDao.size();
			assertEquals(numRegistrosEsperado, respuestaObtenida);
		} catch (SQLException e) {
			fail("El testsize falla" + e.getMessage());
		}
	}

	@Test
	@Order(6)
	void testExists() {
		boolean respuestaObtenida;
		try {
			respuestaObtenida = capaDao.exists(registroExiste1.getId());
			assertTrue(respuestaObtenida);
			respuestaObtenida = capaDao.exists(registroNoExiste.getId());
			assertFalse(respuestaObtenida);
		} catch (SQLException e) {
			fail("El testexists falla" + e.getMessage());
		}
	}

	@Test
	@Order(7)
	void testGetNextLine() {
		int respuestaObtenida;
		try {
			respuestaObtenida = capaDao.getNextLine(registroExiste1.getId());
			assertEquals(5, respuestaObtenida);
			respuestaObtenida = capaDao.getNextLine(1);
			assertEquals(3, respuestaObtenida);
			respuestaObtenida = capaDao.getNextLine(numRegistrosEsperado + 1); // factura sin lineas
			assertEquals(1, respuestaObtenida);
			respuestaObtenida = capaDao.getNextLine(registroNoExiste.getId());
			assertEquals(-1, respuestaObtenida);
		} catch (SQLException e) {
			fail("El textgetnextline falla" + e.getMessage());
		}
	}

	@Test
	@Order(8)
	void testGetImporteTotal() {
		double respuestaObtenida;
		try {
			respuestaObtenida = capaDao.getImporteTotal(registroExiste1.getId());
			assertEquals(112, respuestaObtenida);
			respuestaObtenida = capaDao.getImporteTotal(registroExiste2.getId());
			assertEquals(4308, respuestaObtenida);
			respuestaObtenida = capaDao.getImporteTotal(registroNoExiste.getId());
			assertEquals(-1, respuestaObtenida);
		} catch (SQLException e) {
			fail("El textgetimportetotal falla" + e.getMessage());
		}
		
	}

	@Test
	@Order(9)
	void testFindByExample() {
		assertNull(capaDao.findByExample(registroExiste1));
	}


}
