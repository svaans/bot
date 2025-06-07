import React, { useEffect, useState } from "react";
import axios from "../services/axios"; // Asegúrate que exporta correctamente una instancia
import Estadisticas from "../components/Estadisticas";
import MainLayout from "../layouts/MainLayout";

const API_URL = "/ordenes/"; // Ya usa el baseURL definido en services/axios.js

function Dashboard() {
  const [ordenes, setOrdenes] = useState([]);
  const [ordenesFiltradas, setOrdenesFiltradas] = useState([]);
  const [error, setError] = useState(null);
  const [filtroSymbol, setFiltroSymbol] = useState("");
  const [filtroTipo, setFiltroTipo] = useState("");
  const [filtroFecha, setFiltroFecha] = useState("");

  const fetchOrdenes = async () => {
    try {
      const token = localStorage.getItem("access");
      if (!token) {
        setError("No hay token de acceso.");
        return;
      }

      const res = await axios.get(API_URL, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      setOrdenes(res.data.results);
      setOrdenesFiltradas(res.data.results);
    } catch (err) {
      console.error("Error al obtener órdenes:", err);
      if (err.response?.status === 401) {
        setError("Token inválido o expirado. Vuelve a iniciar sesión.");
      } else {
        setError("No se pudieron cargar las órdenes.");
      }
    }
  };

  const aplicarFiltros = () => {
    const filtradas = ordenes.filter((orden) => {
      const cumpleSymbol = filtroSymbol ? orden.symbol.includes(filtroSymbol) : true;
      const cumpleTipo = filtroTipo ? orden.tipo === filtroTipo : true;
      const cumpleFecha = filtroFecha ? new Date(orden.fecha) >= new Date(filtroFecha) : true;
      return cumpleSymbol && cumpleTipo && cumpleFecha;
    });
    setOrdenesFiltradas(filtradas);
  };

  useEffect(() => {
    fetchOrdenes();
    const intervalo = setInterval(fetchOrdenes, 10000);
    return () => clearInterval(intervalo);
  }, []);

  useEffect(() => {
    aplicarFiltros();
  }, [filtroSymbol, filtroTipo, filtroFecha, ordenes]);

  return (
    <MainLayout>
      <h2 className="text-2xl font-bold mb-4">Órdenes ejecutadas</h2>

      <div className="mb-6 grid grid-cols-1 md:grid-cols-3 gap-4">
        <input
          type="text"
          placeholder="🔎 Símbolo (BTC/EUR)"
          value={filtroSymbol}
          onChange={(e) => setFiltroSymbol(e.target.value)}
          className="p-2 border rounded"
        />
        <select
          value={filtroTipo}
          onChange={(e) => setFiltroTipo(e.target.value)}
          className="p-2 border rounded"
        >
          <option value="">Todos los tipos</option>
          <option value="compra">Compra</option>
          <option value="venta">Venta</option>
        </select>
        <input
          type="date"
          value={filtroFecha}
          onChange={(e) => setFiltroFecha(e.target.value)}
          className="p-2 border rounded"
        />
      </div>

      {error && <p className="text-red-500 font-semibold">{error}</p>}

      {ordenesFiltradas.length === 0 && !error ? (
        <p>No hay órdenes que coincidan.</p>
      ) : (
        <>
          <Estadisticas ordenes={ordenesFiltradas} />

          <table className="w-full mt-4 table-auto border-collapse border border-gray-300">
            <thead>
              <tr className="bg-gray-200">
                <th className="border px-4 py-2">Símbolo</th>
                <th className="border px-4 py-2">Tipo</th>
                <th className="border px-4 py-2">Entrada</th>
                <th className="border px-4 py-2">Salida</th>
                <th className="border px-4 py-2">¿Éxito?</th>
                <th className="border px-4 py-2">Fecha</th>
              </tr>
            </thead>
            <tbody>
              {ordenesFiltradas.map((orden, index) => (
                <tr key={index}>
                  <td className="border px-4 py-2">{orden.symbol}</td>
                  <td className="border px-4 py-2">{orden.tipo}</td>
                  <td className="border px-4 py-2">{orden.precio_entrada}</td>
                  <td className="border px-4 py-2">{orden.precio_salida || "-"}</td>
                  <td className="border px-4 py-2">{orden.exito ? "✅" : "❌"}</td>
                  <td className="border px-4 py-2">
                    {new Date(orden.fecha).toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </MainLayout>
  );
}

export default Dashboard;
