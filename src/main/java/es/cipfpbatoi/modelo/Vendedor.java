package es.cipfpbatoi.modelo;

import java.time.LocalDate;
import java.util.Objects;

public class Vendedor {
	private int id;
	private String nombre;
	private LocalDate fechaIngreso;
	private float salario;

	public Vendedor() {
	}

	public Vendedor(String nombre, LocalDate fechaIngreso, float salario) {
		this.nombre = nombre;
		this.fechaIngreso = fechaIngreso;
		this.salario = salario;
	}

	public Vendedor(int id, String nombre, LocalDate fechaIngreso, float salario) {
		this.id = id;
		this.nombre = nombre;
		this.fechaIngreso = fechaIngreso;
		this.salario = salario;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public LocalDate getFechaIngreso() {
		return fechaIngreso;
	}

	public void setFechaIngreso(LocalDate fechaIngreso) {
		this.fechaIngreso = fechaIngreso;
	}

	public float getSalario() {
		return salario;
	}

	public void setSalario(float salario) {
		this.salario = salario;
	}

	@Override
	public String toString() {
		return "Vendedor [id=" + id + ", nombre=" + nombre + ", fecha_ingreso=" + fechaIngreso + ", salario=" + salario
				+ "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(fechaIngreso, id, nombre, salario);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Vendedor other = (Vendedor) obj;
		return Objects.equals(fechaIngreso, other.fechaIngreso) && id == other.id
				&& Objects.equals(nombre, other.nombre)
				&& Float.floatToIntBits(salario) == Float.floatToIntBits(other.salario);
	}

}
