export function createRuntimeClientProxy<
  TClient extends object,
  TOverrides extends object
>(client: TClient, overrides: TOverrides): TClient & TOverrides {
  return new Proxy(client, {
    get(target, property) {
      if (Object.hasOwn(overrides, property)) {
        return Reflect.get(overrides, property);
      }

      const value = Reflect.get(target, property, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
    getOwnPropertyDescriptor(target, property) {
      return (
        Reflect.getOwnPropertyDescriptor(overrides, property) ??
        Reflect.getOwnPropertyDescriptor(target, property)
      );
    },
    has(target, property) {
      return Object.hasOwn(overrides, property) || Reflect.has(target, property);
    },
    ownKeys(target) {
      return [...new Set([
        ...Reflect.ownKeys(target),
        ...Reflect.ownKeys(overrides)
      ])];
    },
    set(target, property, value) {
      if (Object.hasOwn(overrides, property)) {
        return Reflect.set(overrides, property, value);
      }

      return Reflect.set(target, property, value, target);
    }
  }) as TClient & TOverrides;
}
