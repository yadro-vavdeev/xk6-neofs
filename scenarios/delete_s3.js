import registry from 'k6/x/neofs/registry';
import s3 from 'k6/x/neofs/s3';
import { Counter } from 'k6/metrics';

const obj_registry = registry.open(__ENV.REGISTRY_FILE || 'registry.dat');
const delete_percent = __ENV.DELETE_PERCENT || '2';
const time_limit = __ENV.TIME_LIMIT || "10";
const clients = __ENV.CLIENTS || "2"
const s3_endpoint = __ENV.ENDPOINT || "http://10.78.69.118:8084";
const s3_client = s3.connect(s3_endpoint);

const obj_counters = {
    deleted: new Counter('deleted_obj'),
    skipped: new Counter('skipped_obj'),
    invalid: new Counter('invalid_obj'),
};

const obj_to_delete_selector = registry.getSelector(
    __ENV.REGISTRY_FILE,
    "obj_to_delete",
    __ENV.SELECTION_SIZE ? parseInt(__ENV.SELECTION_SIZE) : 0,
    {
        status: "created",
    }
);
const obj_to_delete_count = obj_to_delete_selector.count();
const iterations = Math.max(1, obj_to_delete_count);
const vus = Math.min(parseInt(clients), iterations);
const scenarios = {};

scenarios.delete = {
    executor: 'shared-iterations',
    vus,
    iterations,
    maxDuration: `${time_limit}s`,
    exec: 'obj_delete',
    gracefulStop: '5s',
};


export const options = {
    scenarios,
    setupTimeout: '5s',
};

export function setup() {
    // Populate counters with initial values
    for (const [status, counter] of Object.entries(obj_counters)) {
        const obj_selector = registry.getSelector(
            __ENV.REGISTRY_FILE,
            status,
            __ENV.SELECTION_SIZE ? parseInt(__ENV.SELECTION_SIZE) : 0,
            { status });
        counter.add(obj_selector.count());
    }
}

export function obj_delete() {
    const obj = obj_to_delete_selector.nextObject();
    if (!obj) {
        console.log("All objects have been deleted");
        return;
    }
    if (delete_percent === '100' || Math.random() < parseInt(delete_percent) / 100) {
        const obj_status = delete_object_with_retries(obj, 1);
        obj_counters[obj_status].add(1);
        obj_registry.setObjectStatus(obj.id, obj_status);
    }
}

function delete_object_with_retries(obj, attempts) {
    for (let i = 0; i < attempts; i++) {
        const result = s3_client.delete(obj.s3_bucket, obj.s3_key);
        if (result.success) {
            return "deleted";
        } else if (result.error === "hash mismatch") {
            return "invalid";
        }
        // Unless we explicitly saw that there was a hash mismatch, then we will retry after a delay
        console.log(`Delete error on ${obj.id}: ${result.error}. Object will be re-tried`);
    }
    return "invalid";
}
