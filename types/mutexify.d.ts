declare module 'mutexify' {
  // Type definitions for mutexify 1.2
  // Project: https://github.com/mafintosh/mutexify
  // Definitions by: Gustav Bylund <https://github.com/maistho>
  // Definitions: https://github.com/DefinitelyTyped/DefinitelyTyped

  function release(): void
  function release(
    cb: (err?: any, value?: any) => any,
    err: any,
    value: any
  ): void

  interface Lock {
    (fn: typeof release): number
    locked: boolean
  }

  function mutexify(): Lock

  export = mutexify
}
