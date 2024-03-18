local pb = require 'pb'

local inp = io.open("otel.pb", "rb")
local data = inp:read("*all")

local status, offset = pb.load(data)
if not status then
    print(("error %s during load"):format(offset))
end

for name, basename, type in pb.types() do
    print(name, basename, type)
end

