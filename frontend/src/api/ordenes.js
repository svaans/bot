import axios from 'axios';

const API_URL = "http://127.0.0.1:8000/api/ordenes/";

export const obtenerOrdenes = async (token) => {
  const respuesta = await axios.get(API_URL, {
    headers: {
      Authorization: `Bearer ${token}`
    }
  });
  return respuesta.data;
};