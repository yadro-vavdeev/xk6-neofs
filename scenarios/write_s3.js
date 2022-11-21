import datagen from 'k6/x/neofs/datagen';
import registry from 'k6/x/neofs/registry';
import s3 from 'k6/x/neofs/s3';


const s3_endpoint = __ENV.ENDPOINT || "https://dev-vavdeev.spb.yadro.com:8080";
const s3_client = s3.connect(s3_endpoint);

const clients = __ENV.CLIENTS || '2';
const validate_percent = __ENV.VALIDATE_PERCENT || '2';
const obj_registry = registry.open(__ENV.REGISTRY_FILE || 'registry.dat');
const obj_size = __ENV.WRITE_OBJ_SIZE || '1'
const buckets = __ENV.BUCKETS.split(',');
const duration = __ENV.DURATION || '30'

const generator = datagen.generator(1024 * parseInt(obj_size));
const scenarios = {};

scenarios.write = {
    executor: 'constant-vus',
    vus: Math.min(1, parseInt(clients)),
    duration: `${duration}s`,
    exec: 'obj_write',
    gracefulStop: '5s',
};

export const options = {
    scenarios,
    setupTimeout: '5s',
    insecureSkipTLSVerify: true,
};

export function obj_write() {
    const key = uuidv4();
    const bucket = buckets[Math.floor(Math.random() * buckets.length)];
    const registry_enabled = Math.random() < parseInt(validate_percent) / 100;
    const { payload, hash } = generator.genPayload(registry_enabled);
    const resp = s3_client.put(bucket, key, payload);
    if (!resp.success) {
        console.log(resp.error);
    }
    if (registry_enabled) {
        obj_registry.addObject("", "", bucket, key, hash);
    }
}

export function uuidv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        let r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

export function teardown(data) {
    if (obj_registry) {
        obj_registry.close();
    }
}
