import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { loginUsuario } from "../services/auth";
import { useAuth } from "../context/AuthContext";

function Login() {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState(null);
  const navigate = useNavigate();
  const { login } = useAuth(); // ✅ Aquí usamos el login del contexto

  const handleLogin = async (e) => {
    e.preventDefault();
    try {
      const token = await loginUsuario(username, password); // ✅ OBTIENE el string
      login(token); // ✅ GUARDA el token en localStorage
      navigate("/dashboard"); // ✅ Redirige tras guardar
    } catch (err) {
      setError(err.message || "Error al iniciar sesión");
    }
  };


  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-blue-900 to-indigo-700 px-4">
      <div className="bg-white p-8 rounded-lg shadow-lg w-full max-w-sm">
        <h2 className="text-2xl font-bold text-center text-gray-800 mb-6">Iniciar sesión</h2>

        {error && (
          <div className="mb-4 text-red-500 text-center text-sm font-semibold">
            {error}
          </div>
        )}

        <form onSubmit={handleLogin} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700">Usuario</label>
            <input
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="mt-1 p-2 w-full border rounded-md shadow-sm focus:outline-none focus:ring focus:border-indigo-300"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700">Contraseña</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="mt-1 p-2 w-full border rounded-md shadow-sm focus:outline-none focus:ring focus:border-indigo-300"
              required
            />
          </div>

          <button
            type="submit"
            className="w-full bg-blue-600 hover:bg-blue-700 text-white py-2 rounded font-semibold"
          >
            Ingresar
          </button>
        </form>
      </div>
    </div>
  );
}

export default Login;
