import registry from 'k6/x/neofs/registry';
import s3 from 'k6/x/neofs/s3';
import { Counter } from 'k6/metrics';

const obj_registry = registry.open(__ENV.REGISTRY_FILE || 'registry.dat');
const time_limit = __ENV.TIME_LIMIT || "10";
const clients = __ENV.CLIENTS || "2"
const s3_endpoint = __ENV.ENDPOINT || "https://dev-vavdeev.spb.yadro.com:8080";
const s3_client = s3.connect(s3_endpoint);

const obj_counters = {
    verified: new Counter('verified_obj'),
    skipped: new Counter('skipped_obj'),
    invalid: new Counter('invalid_obj'),
};

const obj_to_verify_selector = registry.getSelector(
    __ENV.REGISTRY_FILE,
    "obj_to_verify",
    __ENV.SELECTION_SIZE ? parseInt(__ENV.SELECTION_SIZE) : 0,
    {
        status: "created",
    }
);
const obj_to_verify_count = obj_to_verify_selector.count();
const iterations = Math.max(1, obj_to_verify_count);
const vus = Math.min(parseInt(clients), iterations);
const scenarios = {};

scenarios.verify = {
    executor: 'shared-iterations',
    vus,
    iterations,
    maxDuration: `${time_limit}s`,
    exec: 'obj_verify',
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

export function obj_verify() {
    const obj = obj_to_verify_selector.nextObject();
    if (!obj) {
        console.log("All objects have been verified");
        return;
    }

    const obj_status = verify_object_with_retries(obj, 1);
    obj_counters[obj_status].add(1);
    obj_registry.setObjectStatus(obj.id, obj_status);
}

function verify_object_with_retries(obj, attempts) {
    for (let i = 0; i < attempts; i++) {
        const result = s3_client.verifyHash(obj.s3_bucket, obj.s3_key, obj.payload_hash);
        if (result.success) {
            return "verified";
        } else if (result.error === "hash mismatch") {
            return "invalid";
        }
        // Unless we explicitly saw that there was a hash mismatch, then we will retry after a delay
        console.log(`Verify error on ${obj.id}: ${result.error}. Object will be re-tried`);
    }
    return "invalid";
}
