export enum PipelineState {
  IDLE = "idle",
  EXEC = "exec",
  WAIT = "wait",
  DONE = "done",
}

export type PipelineFunction = (args: any, scope?: any, global?: any) => any

export type PipelineFunctionMap = { [key: string]: PipelineFunction }

export type PipelineModuleMap = { [key: string]: () => Promise<PipelineFunctionMap> }

export type PipelineRunConfig = {
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
    public args: Record<string, any>,
    public fanIn: string[],
    public fanOut: string[],
    public entrypoint: boolean,
  ) {}
  
  fanInCheck: string[] = []
  fanOutPending: string | null = null
  state: PipelineState = PipelineState.IDLE
  input: PipelineInput = {}
  output: PipelineOutput = null

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

  complete(input: PipelineInput, args: PipelineInput, output: PipelineOutput) {
    this.fanInCheck = [...this.fanIn]
    this.state = PipelineState.DONE
    this.input = {...input}
    this.args = {...args}
    this.output = [...(output ?? [])]
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
}

export type PipelineEventListener = {
  id: number
  pipeline: string | null
  callback: PipelineEventCallback
  state: PipelineState | null
}

export type PipelineEventCallback = (pipeline: Pipeline) => void

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

  emit(pipeline: Pipeline) {
    this.listeners
      .filter(e => (e.pipeline == pipeline.functionName || e.pipeline == null)
                && (e.state == pipeline.state || !e.state))
      .forEach(e => e.callback(pipeline))
  }
}
