module.exports.Lite_to_normal = function Lite_to_normal(compactMessage) {
  if (!compactMessage || !Array.isArray(compactMessage.d)) {
    throw new Error("Invalid compact message format");
  }

  const verbose = {
    Protocol: compactMessage.p || "IPSO_v1",
    Data: []
  };

  for (const entry of compactMessage.d) {
    if (!Array.isArray(entry) || entry.length !== 2) {
      continue; // skip malformed entries
    }

    const [path, value] = entry;

    if (typeof path !== "string") {
      continue;
    }

    const parts = path.split("/");

    if (parts.length !== 3) {
      continue; // skip malformed paths
    }

    const [object, instanceStr, resource] = parts;

    const instance = parseInt(instanceStr, 10);

    verbose.Data.push({
      Object: object,
      Instance: isNaN(instance) ? 0 : instance,
      Resource: resource,
      Value: value
    });
  }

  return verbose;
}
