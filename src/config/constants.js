// src/config/constants.js - Constantes de configuración
require('dotenv').config();

const ACOLCHADO_DIAS = Number(process.env.ACOLCHADO_DIAS || 180);
const DEBUG_REF_ID = process.env.DEBUG_REF_ID ? Number(process.env.DEBUG_REF_ID) : null;

module.exports = { ACOLCHADO_DIAS, DEBUG_REF_ID };
