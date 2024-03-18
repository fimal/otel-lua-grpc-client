
local protoc = require 'protoc'

local function file_exists(file)
    local fp = io.open(file, "r")
    if fp then
      fp:close()
      return true
    end
    return false
end

local p = protoc.new()
local proto_file = "opentelemetry/proto/collector/logs/v1/logs_service.proto"

if not file_exists(proto_file) then
    print(("pb file: %s is not found"):format(proto_file))
end
-- local PROTOC_IMPORT_PATHS = {"."}
local import_paths = {"."}
-- for _, v in ipairs(PROTOC_IMPORT_PATHS or {}) do
--     table.insert(import_paths, v)
-- end
p.paths = import_paths
p.include_imports = true
print ("file name: " .. proto_file)
-- local descriptor_proto = p:parsefile(proto_file)
print("Now create binary compiled pb file from proto schema")

local data = p:compilefile(proto_file)

local out = io.open("otel.pb", "wb")
out:write(data)
out:close()