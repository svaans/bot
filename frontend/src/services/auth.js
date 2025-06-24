// services/auth.js
import axios from "axios";

const API_URL = "http://127.0.0.1:8000/api";

export async function loginUsuario(username, password) {
  try {
    const response = await axios.post(`${API_URL}/token/`, {
      username,
      password,
    });

    return response.data.access; // Solo devuelve el token
  } catch (error) {
    throw new Error("Credenciales inválidas o error de conexión");
  }
}

export async function registrarUsuario(username, email, password) {
  try {
    const response = await axios.post(`${API_URL}/register/`, {
      username,
      email,
      password,
    });

    return response.data;
  } catch (error) {
    throw new Error("Error al registrar usuario");
  }
}

export function isAuthenticated() {
  const token = localStorage.getItem("access"); // ✅ Correcto
  return !!token; // ✅ Retorna true o false
}
