import React from "react";
import {
  BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from "recharts";

function Estadisticas({ ordenes }) {
  if (!ordenes || ordenes.length === 0) return <p>No hay datos para mostrar.</p>;

  // Agrupar por fecha (ganancia por día)
  const resumen = {};
  let exitosas = 0;

  ordenes.forEach((orden) => {
    const fecha = new Date(orden.fecha).toLocaleDateString();
    const ganancia = orden.precio_salida && orden.precio_entrada
      ? (orden.tipo === "compra"
        ? orden.precio_salida - orden.precio_entrada
        : orden.precio_entrada - orden.precio_salida)
      : 0;

    if (!resumen[fecha]) {
      resumen[fecha] = { fecha, ganancia: 0, operaciones: 0 };
    }
    resumen[fecha].ganancia += ganancia;
    resumen[fecha].operaciones += 1;
    if (orden.exito) exitosas += 1;
  });

  const datos = Object.values(resumen);
  const total = ordenes.length;
  const winrate = total ? ((exitosas / total) * 100).toFixed(1) : 0;

  return (
    <div className="mt-10">
      <h2 className="text-xl font-semibold mb-4">📊 Estadísticas</h2>
      <p className="mb-2">✅ Winrate: {winrate}%</p>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={datos}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="fecha" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Bar dataKey="ganancia" fill="#82ca9d" />
          <Bar dataKey="operaciones" fill="#8884d8" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

export default Estadisticas;
