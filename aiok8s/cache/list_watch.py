class ListWatch:
    def __init__(self, list_func, watch_func):
        self.list_func = list_func
        self.watch_func = watch_func

    async def list(self, options):
        return await self.list_func(options)

    async def watch(self, options):
        return await self.watch_func(options)
