export enum PipelineState {
  IDLE = "idle",
  EXEC = "exec",
  WAIT = "wait",
  DONE = "done",
  FAILED = "failed",
  SKIP = "skip",
}

export type PipelineFunction = (args: any, scope?: any, global?: any) => any

export type PipelineFunctionMap = { [key: string]: PipelineFunction }

export type PipelineModuleMap = { [key: string]: () => Promise<PipelineFunctionMap> }

export type PipelineRunConfig = {
  id?: number
  args?: any
  scope?: any
  global?: any
}

export type PipelineInput = Record<string, any>

export type PipelineOutput = Record<string, any>[] | null

export class Pipeline {
  constructor(
    public name: string,
    public functionName: string,
    public path: string | null,
    public workflow: string,
    public args: PipelineInput,
    public fanIn: string[],
    public fanOut: string[],
    public entrypoint: boolean,
  ) {}
  
  fanInCheck: string[] = []
  fanOutPending: string | null = null
  state: PipelineState = PipelineState.IDLE
  input: PipelineInput = {}
  output: PipelineOutput = null
  error: string | null = null

  static fromJson(json: any) {
    return new Pipeline(
      json['name'],
      json['function'],
      json['path'],
      json['workflow'],
      json['args'],
      json['fanIn'],
      json['fanOut'],
      json['entrypoint'],
    )
  }

  parseInput(value: PipelineInput): PipelineInput {
    if (typeof value !== 'object' || Object.getPrototypeOf(value) != Object.prototype) {
      throw Error(`Pipeline ${this} input expects a plain object, but received '${value}' (${typeof value}).`)
    }
    return { ...this.input, ...value }
  }

  isReady() {
    const fanInSet = new Set(this.fanIn)
    const fanInCheckSet = new Set(this.fanInCheck)
    if (fanInSet.size !== fanInCheckSet.size) return false
    return [...fanInSet].every(value => fanInCheckSet.has(value))
  }

  canBeExecuted() {
    return [PipelineState.IDLE, PipelineState.WAIT].includes(this.state)
  }

  copy(pipeline: Pipeline) {
    this.fanInCheck = pipeline.state == PipelineState.DONE ? [...this.fanIn] : this.fanInCheck
    this.state = pipeline.state
    this.input = {...pipeline.input}
    this.args = {...pipeline.args}
    this.output = [...(pipeline.output ?? [])]
  }

  reset() {
    return new Pipeline(
      this.name,
      this.functionName,
      this.path,
      this.workflow,
      this.args,
      this.fanIn,
      this.fanOut,
      this.entrypoint,
    )
  }

  toString() {
    return this.path
      ? `[${this.name}]`
      : `[${this.workflow}.${this.name}]`
  }
}

export type PipelineEventListener = {
  id: number
  pipeline: string | null
  callback: PipelineEventCallback
  state: PipelineState | null
}

export type PipelineEventCallback = (pipeline: Pipeline, config?: PipelineRunConfig) => void

export type PipelineEventUnsubscribe = () => void

export class PipelineEventEmitter {
  constructor() {}

  private listeners: PipelineEventListener[] = []

  on(
    pipeline: string | null,
    callback: PipelineEventCallback,
    state: PipelineState | null = PipelineState.DONE,
  ): PipelineEventUnsubscribe {
    const id = Date.now()
    this.listeners.push({ id, pipeline, callback, state })
    return () => {
      const index = this.listeners.findIndex(e => e.id == id)
      this.listeners.splice(index, 1)
    }
  }

  emit(pipeline: Pipeline, config?: PipelineRunConfig) {
    this.listeners
      .filter(e => (e.pipeline == pipeline.functionName || e.pipeline == null)
                && (e.state == pipeline.state || !e.state))
      .forEach(e => e.callback(pipeline, config))
  }
}
