"""Implementación interna del pipeline de velas.

La API estable para el resto del proyecto sigue siendo :mod:`core.procesar_vela`
(fachada que reexporta desde :mod:`core.vela.pipeline`). La lógica auxiliar vive
en submódulos (:mod:`core.vela.buffers`, :mod:`core.vela.helpers`, etc.).
"""
