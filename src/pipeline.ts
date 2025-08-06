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
  input: Record<string, any> = {}
  output: Record<string, any>[] | null = null

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

  copyState(pipeline: Pipeline) {
    this.fanInCheck = [...pipeline.fanInCheck]
    this.state = pipeline.state
    this.input = {...pipeline.input}
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
}
