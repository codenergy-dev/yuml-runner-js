import { PipelineState } from '../src/pipeline'
import { Workflows } from '../src/workflows'
import { a } from './pipelines/a-b'
import { readWorkflowJson } from './utils/read-workflow-json'

describe('Workflows', () => {
  it('parse workflow', () => {
    const workflows = Workflows.fromJson(readWorkflowJson('a-b.json'))
    expect(workflows.pipelines.length).toBe(2)
  })
  
  it('bind workflow modules and run pipelines', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('a-b.json'))
    workflows.bindModules({
      'a-b': () => import('./pipelines/a-b')
    })

    const pipelines = await workflows.run(a)
    expect(pipelines.filter(p => p.state == PipelineState.DONE).length).toBe(2)
  })
  
  it('validate pipeline args', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('a-b.json'))
    workflows.bindModules({
      'a-b': () => import('./pipelines/a-b')
    })

    const pipelines = await workflows.run(a)
    expect(pipelines.find(p => p.name == 'a')?.args['foo']).toBe('bar')
  })

  it('validate pipelines execution sequence', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('a-b.json'))
    workflows.bindModules({
      'a-b': () => import('./pipelines/a-b')
    })

    const history: string[] = []
    workflows.events.on(null, (pipeline) => history.push(pipeline.name))

    await workflows.run(a)
    
    expect(history[0]).toBe('a')
    expect(history[1]).toBe('b')
  })
})