local pb = require("pb")
local protoc = require("protoc")
local bit = require("bit")

local json
if not os.getenv("LUAUNIT") then
  json = require("cjson")
end
local http_client = require("http.client")

local package = "opentelemetry.proto.collector.logs.v1"
local service_request = "ExportLogsServiceRequest"
local method = "Export"
local service = "LogsService"
local proto_file = "opentelemetry/proto/collector/logs/v1/logs_service.proto"
local tcp_port = "9000"
local host = "127.0.0.1"

local response_status = "404"
local response_key = "response_code"
local enforcer_descriptors_key = "descriptors"
local enforcer_descriptors = "[{\"profile\": \"test/waas-api-profile\",\"tag\": \"enforcer\",\"policy\": \"apiPolicy\",\"source\": \"222.222.223.111\",\"classifier\": \"httpbin-sample\",\"actor\": \"\",\"hash\": \"cs:true;sp:false\",\"rate\": \"5m\",\"rule_name\": \"404-status-code\",\"field\": \"response_code\",\"values\": \"404;\",\"block_period\": \"5m\"}]"

local function find_method(protos, my_package, my_service, my_method)
    for k, loaded in pairs(protos) do
      if type(loaded) == "boolean" then
        print(k)
      end
      local package = loaded.package
      for _, s in ipairs(loaded.service or {}) do
        if ("%s.%s"):format(package, s.name) ==  ("%s.%s"):format(my_package, my_service) then
          for _, m in ipairs(s.method) do
            if m.name == my_method then
              return m
            end
          end
        end
      end
    end
    return nil
end
local function file_exists(file)
    local fp = io.open(file, "r")
    if fp then
      fp:close()
      return true
    end
    return false
end
local function printTable( t )
    local printTable_cache = {}
    local function sub_printTable( t, indent )
        if ( printTable_cache[tostring(t)] ) then
            print( indent .. "*" .. tostring(t) )
        else
            printTable_cache[tostring(t)] = true
            if ( type( t ) == "table" ) then
                for pos,val in pairs( t ) do
                    if ( type(val) == "table" ) then
                        print( indent .. "[" .. pos .. "] => " .. tostring( t ).. " {" )
                        sub_printTable( val, indent .. string.rep( " ", string.len(pos)+8 ) )
                        print( indent .. string.rep( " ", string.len(pos)+6 ) .. "}" )
                    elseif ( type(val) == "string" ) then
                        print( indent .. "[" .. pos .. '] => "' .. val .. '"' )
                    else
                        print( indent .. "[" .. pos .. "] => " .. tostring(val) )
                    end
                end
            else
                print( indent..tostring(t) )
            end
        end
    end
    if ( type(t) == "table" ) then
        print( tostring(t) .. " {" )
        sub_printTable( t, "  " )
        print( "}" )
    else
        sub_printTable( t, "  " )
    end    
end
protoc.reload()
-- Build message table
local response_status_table = {}
response_status_table["key"] = response_key
response_status_table["value"] = { string_value = response_status }

local enforcer_descriptors_table = {}
enforcer_descriptors_table["key"] = enforcer_descriptors_key
enforcer_descriptors_table["value"] = { string_value = enforcer_descriptors}

local my_table = {resource_logs={{scope_logs={{log_records={{attributes={ enforcer_descriptors_table, response_status_table } } } } } } } }
-- Load proto
local p = protoc.new()
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

p:loadfile(proto_file)
if p.loaded then
    print (("pb file %s loaded succesefully"):format(proto_file))
end
local m = find_method(p.loaded, package, service, method)
if not m then
  print (("Undefined service method: %s/%s"):format(service, method))
end

-- local my_table = json.decode(message)
print(("Input proto type: %s"):format(m.input_type))
local bytes = pb.encode(m.input_type, my_table)
local size = string.len(bytes)

-- Prepend gRPC specific prefix data
-- request is compressed (always 0)
-- request body size (4 bytes)
local prefix = {
    string.char(0),
    string.char(bit.band(bit.rshift(size, 24), 0xFF)),
    string.char(bit.band(bit.rshift(size, 16), 0xFF)),
    string.char(bit.band(bit.rshift(size, 8), 0xFF)),
    string.char(bit.band(size, 0xFF))
  }
local my_grpc_message = table.concat(prefix) .. bytes

local uri = ("http://%s:%s/%s.%s/%s"):format(host,tcp_port,package,service,method)
local req = request.new_from_uri(uri)

if my_grpc_message then
    req.version = 2
    req.headers:upsert(":method", "POST")
    req.headers:upsert("content-type", "application/grpc")
	  req:set_body(my_grpc_message)
end
print("# REQUEST")
print("## HEADERS")
for k, v in req.headers:each() do
	print(k, v)
end
print()
local req_timeout = 10
local headers, stream = req:go(req_timeout)