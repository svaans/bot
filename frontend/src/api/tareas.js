import axios from "axios";

export const obtenerTareas = async (token) => {
  const respuesta = await axios.get("http://127.0.0.1:8000/api/tareas/", {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });
  return respuesta.data;
};
