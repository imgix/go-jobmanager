local levee = require("levee")
local errors = require("levee.errors")

local running = true
local hub = levee.Hub()
local stdin = hub.io:stdin()
local instream = stdin:stream()

while running do
	local err, line = instream:line("\n")
	if err and err == errors.CLOSED then 
		running = false
	end
end
