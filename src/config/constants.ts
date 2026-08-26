import 'dotenv/config';

const ACOLCHADO_DIAS = Number(process.env.ACOLCHADO_DIAS ?? 180);
const DEBUG_REF_ID = process.env.DEBUG_REF_ID ? Number(process.env.DEBUG_REF_ID) : null;

export { ACOLCHADO_DIAS, DEBUG_REF_ID };
