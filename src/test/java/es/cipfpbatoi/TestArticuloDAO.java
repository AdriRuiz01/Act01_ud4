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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import es.cipfpbatoi.dao.ArticuloDAO;
import es.cipfpbatoi.dao.ConexionBD;
import es.cipfpbatoi.modelo.Articulo;

@TestMethodOrder(OrderAnnotation.class)
class TestArticuloDAO {
	static ArticuloDAO capaDao;
	Articulo registroVacio = new Articulo();
	Articulo registroExiste1 = new Articulo(1, "Monitor 20", 178f, "mon20", 1);
	Articulo registroExiste2 = new Articulo(5, "Papel A4-500", 4f, "PA4500", 2);
	Articulo registroNoExiste = new Articulo(100, "No existe", 0f, "ne", 1);
	Articulo registroNuevo = new Articulo("insert test", 100f, "instest", 1);
	Articulo registroNuevoError = new Articulo("insert test", 100f, "instest", 100);
	Articulo registroModificarBorrar = new Articulo(9, "update test", 100f, "updtest", 1);
	Articulo registroModificarBorrarError = new Articulo(9, "update test", 100f, "updtest", 100);
	static int numRegistrosEsperado = 9;
	static int autoIncrement = 9;
	final static String TABLA = "articulos";
	final static String BD = "empresa_ad_test";

	@BeforeAll
	static void setUpBeforeClass() {
		try {
			capaDao = new ArticuloDAO();

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
					+ "(nombre, precio, codigo, grupo) values ('nombre test', 100, 'nomtest', 1)");

		} catch (SQLException e) {
			fail("El test falla al preparar el test (instanciando dao: posiblemente falla la conexión a la BD)" + e.getMessage());
		}
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		capaDao.cerrar();
	}

	@Test
	@Order(1)
	void testfind() {
		Articulo registroObtenido;
		try {
			registroObtenido = capaDao.find(registroExiste1.getId());
			Articulo registroEsperado = registroExiste1;
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
			numRegistrosObtenido = capaDao.findAll().size();
			assertEquals(numRegistrosEsperado, numRegistrosObtenido);
		} catch (SQLException e) {
			fail("El testfindall falla" + e.getMessage());
		}

	}

	@Test
	@Order(2)
	void testInsert() {
		try {
			Articulo registro = capaDao.insert(registroNuevo);
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
	@Order(1)
	void testFindExample() {
		int numRegistrosObtenido;
		try {
			numRegistrosObtenido = capaDao.findByExample(registroVacio).size();
			assertEquals(numRegistrosEsperado, numRegistrosObtenido);

			numRegistrosObtenido = capaDao.findByExample(registroExiste1).size();
			assertEquals(1, numRegistrosObtenido);

			Articulo registro = new Articulo("Monitor", 0f, null, 0);
			numRegistrosObtenido = capaDao.findByExample(registro).size();
			assertEquals(3, numRegistrosObtenido);

			registro = new Articulo("Monitor", 200f, null, 0);
			numRegistrosObtenido = capaDao.findByExample(registro).size();
			assertEquals(2, numRegistrosObtenido);

			registro = new Articulo(null, 60f, null, 0);
			numRegistrosObtenido = capaDao.findByExample(registro).size();
			assertEquals(4, numRegistrosObtenido);

			registro = new Articulo(null, 60f, null, 1);
			numRegistrosObtenido = capaDao.findByExample(registro).size();
			assertEquals(2, numRegistrosObtenido);

			registro = new Articulo(null, 0f, null, 1);
			numRegistrosObtenido = capaDao.findByExample(registro).size();
			assertEquals(7, numRegistrosObtenido);
		} catch (SQLException e) {
			fail("El testfindexameple falla" + e.getMessage());
		}
	}

	@Test
	@Order(1)
	void testFindByGrupo() {
		int numRegistrosObtenido;
		try {
			numRegistrosObtenido = capaDao.findByGrupo(1000).size();
			assertEquals(0, numRegistrosObtenido);

			numRegistrosObtenido = capaDao.findByGrupo(1).size();
			assertEquals(7, numRegistrosObtenido);

			numRegistrosObtenido = capaDao.findByGrupo(2).size();
			assertEquals(1, numRegistrosObtenido);
		} catch (SQLException e) {
			fail("El testfindgrupo falla" + e.getMessage());
		}
	}

}
