import {pathToFileURL} from 'node:url';
import {keyFromHex,openState} from './state.mjs';
export async function initialBody(hex){
  let body;const db=openState({key:keyFromHex(hex),initialize:true,persist:async value=>{body=value;}});
  try{await db.save();return body;}finally{db.close();}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
 initialBody(process.env.OBSERVER_STATE_KEY).then(body=>process.stdout.write(body+'\n')).catch(()=>{console.error('state_initialization_failed');process.exitCode=1;});
}
