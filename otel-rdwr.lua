local pb = require 'pb'
local protoc = require 'protoc'
local request = require "http.request"
local bit = require("bit")
local json
if not os.getenv("LUAUNIT") then
  json = require("cjson")
end
local http_client = require "http.client"



local service = "opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest"
local method = "Export"

local function map_message(field, default_values)
    if not pb.type(field) then
      print(("Field %s is not defined"):format(field))
    end
    -- Converts the incomming value based on the protobuf field type
    local function set_value_type(name, kind, request_table)
        local prefix = kind:sub(1, 3)
        if prefix == "str" then
        return request_table[name] or nil
        elseif prefix == "int" then
        if request_table[name] then
            return tonumber(request_table[name])
        else
            return nil
        end
        end
        return nil
    end
    local request = {}
    for name, _, field_type,_,lbl in pb.fields(field) do
      -- print(name)
      -- Find the actual type of field (enum,message, or map)
      local _,_, actualType = pb.type(field_type)
      if field_type:sub(1, 1) == "." then
        -- If a request contains nested messages and make use of the 'repeated' protobuf label we may have to iterate over each inner element 
        -- For each pair of key/values in the table we will recurse and set the correct type of the data i.e string or int and construct the lua request table as normal
        if lbl == "repeated" then
          request[name] = {}
          for _,value in ipairs(default_values[name] or {}) do
           local sub, err = map_message(field_type, value)
           if err then
             print(err)
           end
           table.insert(request[name],sub)
         end
        -- Add support for a enum type, if the default values[name] contains a enum value that is non-existant in the enum definition from the proto file we simply ignore it
        -- Note that enum values can be either string or int from the json that is passed in.
        elseif actualType == "enum" then
          local value = pb.enum(field_type,default_values[name])
          if value ~= nil then
            request[name] = value
          end
       else
         local sub, err = map_message(field_type, default_values[name] or {})
         if err then
           print(err)
         end
         request[name] = sub
       end
      else
        request[name] = set_value_type(name,field_type,default_values) or default_values[name]  or nil
      end
    end
    return request, nil
end
local function find_method(proto,my_service,my_method)
    service = "opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest"
    method = "Export"
    local protos = proto
    for k, loaded in pairs(protos) do
      if type(loaded) == "boolean" then
        print(k)
      end
      local package = loaded.package
      print(package)
      for _, s in ipairs(loaded.service or {}) do
        print(s.name)
        if ("%s.%s"):format(package, s.name) == "opentelemetry.proto.collector.logs.v1." .. my_service then
          for _, m in ipairs(s.method) do
            if m.name == my_method then
              print(m.name )
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
-- local schema_file = "person.pb-lua.cc"
-- if not file_exists(schema_file) then
--     print(("schema file: %s is not found"):format(schema_file))
-- end
-- pb.load(schema_file)
-- local message = [[ {"resource_logs":[{"scope_logs":[{"log_records":[{"attributes":[{"key":"response_code","value":{"string_value":"404"}}]}]}]}]} ]]
-- local message = [[ {} ]]
local message = [[ {"resource_logs":[{"scope_logs":[{"log_records":[{"attributes":[{"key":"descriptors","value":{"string_value":"[{\"profile\": \"test/waas-api-profile\",\"tag\": \"enforcer\",\"policy\": \"apiPolicy\",\"source\": \"222.222.223.111\",\"classifier\": \"httpbin-sample\",\"actor\": \"\",\"hash\": \"cs:true;sp:false\",\"rate\": \"5m\",\"rule_name\": \"404-status-code\",\"field\": \"response_code\",\"values\": \"404;\",\"block_period\": \"5m\"}]"}},{"key":"response_code","value":{"string_value":"404"}}]}]}]}]} ]]
-- local _message = "{\"resource_logs\":[{\"scope_logs\":[{\"log_records\":[{\"attributes\":[{\"key\":\"descriptors\",\"value\":{\"string_value\":\"[{\"profile\": \"test/waas-api-profile\",\"tag\": \"enforcer\",\"policy\": \"apiPolicy\",\"source\": \"1.1.1.1\",\"classifier\": \"httpbin-sample\",\"actor\": \"\",\"hash\": \"cs:true;sp:false\",\"rate\": \"5m\",\"rule_name\": \"404-status-code\",\"field\": \"response_code\",\"values\": \"404;\",\"block_period\": \"5m\"}]\"}},{\"key\":\"response_code\",\"value\":{\"string_value\":\"404\"}}]}]}]}]}"

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
--printTable(descriptor_proto)
p:loadfile(proto_file)
if p.loaded then
    print (("pb file %s loaded succesefully"):format(proto_file))
end
local m = find_method(p.loaded, "LogsService", "Export")
if not m then
  print (("Undefined service method: %s/%s"):format(service, method))
end

local my_table = json.decode(message)

print("start encoding")
print(("Input proto types: %s"):format(m.input_type))

--local bytes = pb.encode(m.input_type, default_values)
-- local request, _ = (map_message(m.input_type, my_table))
-- printTable(request)
-- printTable(default_values)
-- local bytes = pb.encode(m.input_type, map_message(m.input_type, default_values or {}))
local bytes = pb.encode("opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest", my_table)
-- print(("My method : %s and input type %s"):format(m.name,m.input_type))

print(pb.tohex(bytes))
local size = string.len(bytes)
print("FINISHED, size is: " .. size)

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
printTable(prefix)

local my_grpc_message = table.concat(prefix, "") .. bytes

-- local myconnection = http_client.connect {
--     host = "127.0.0.1";
--     port = 31017;
--     tls = false;
--     version = 2 ;
-- }

local uri = ("http://127.0.0.1:31017/opentelemetry.proto.collector.logs.v1.LogsService/Export")
local req_body = bytes
local req = request.new_from_uri(uri)
req.version = 2

if req_body then
	req.headers:upsert(":method", "POST")
    req.headers:upsert("content-type", "application/grpc")
    req.headers:upsert(":scheme", "application/grpc")
	req:set_body(my_grpc_message)
end
print("# REQUEST")
print("## HEADERS")
print(pb.tohex(my_grpc_message))
for k, v in req.headers:each() do
	print(k, v)
end
print()
local req_timeout = 10
local headers, stream = req:go(req_timeout)