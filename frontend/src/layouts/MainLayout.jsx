import { useAuth } from "../context/AuthContext";
import { useNavigate } from "react-router-dom";

export default function MainLayout({ children }) {
  const { usuario, logout } = useAuth();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate("/login");
  };

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Barra superior */}
      <header className="bg-blue-800 text-white p-4 flex justify-between items-center shadow">
        <h1 className="text-xl font-bold">📊 Dashboard del Bot</h1>
        <div className="flex items-center gap-4">
          <span>👤 {usuario?.username || "Invitado"}</span>
          <button
            onClick={handleLogout}
            className="bg-red-600 hover:bg-red-700 px-4 py-1 rounded"
          >
            Cerrar sesión
          </button>
        </div>
      </header>

      {/* Contenido dinámico */}
      <main className="p-6">{children}</main>
    </div>
  );
}
