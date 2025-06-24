import { useState } from 'react';

export default function Register() {
  const [formData, setFormData] = useState({
    username: '',
    password: '',
    email: '',
  });

  const [mensaje, setMensaje] = useState('');
  const [error, setError] = useState('');

  const handleChange = (e) => {
    setFormData((prev) => ({
      ...prev,
      [e.target.name]: e.target.value,
    }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setMensaje('');

    try {
      const response = await fetch('http://127.0.0.1:8000/api/register/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(formData),
      });

      if (!response.ok) throw new Error('No se pudo registrar el usuario');

      setMensaje('✅ Usuario registrado correctamente. Ahora puedes iniciar sesión.');
    } catch (err) {
      setError(err.message);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-purple-800 to-indigo-900 px-4">
      <div className="bg-white p-8 rounded-lg shadow-md w-full max-w-sm">
        <h2 className="text-2xl font-bold text-center text-gray-800 mb-6">Registro de Usuario</h2>

        {mensaje && <div className="mb-4 text-green-600 text-sm text-center">{mensaje}</div>}
        {error && <div className="mb-4 text-red-500 text-sm text-center">{error}</div>}

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm text-gray-700">Usuario</label>
            <input
              type="text"
              name="username"
              onChange={handleChange}
              required
              className="mt-1 p-2 w-full border rounded-md focus:outline-none focus:ring focus:border-indigo-300"
            />
          </div>

          <div>
            <label className="block text-sm text-gray-700">Correo electrónico</label>
            <input
              type="email"
              name="email"
              onChange={handleChange}
              className="mt-1 p-2 w-full border rounded-md focus:outline-none focus:ring focus:border-indigo-300"
            />
          </div>

          <div>
            <label className="block text-sm text-gray-700">Contraseña</label>
            <input
              type="password"
              name="password"
              onChange={handleChange}
              required
              className="mt-1 p-2 w-full border rounded-md focus:outline-none focus:ring focus:border-indigo-300"
            />
          </div>

          <button
            type="submit"
            className="w-full bg-indigo-600 hover:bg-indigo-700 text-white py-2 rounded font-semibold"
          >
            Registrarse
          </button>
        </form>
      </div>
    </div>
  );
}

