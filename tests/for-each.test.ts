import { Workflows } from '../src/workflows'
import { ForEachCount } from './pipelines/for-each'
import { readWorkflowJson } from './utils/read-workflow-json'

describe('for-each', () => {
  it('validate loop execution count', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('for-each.json'))
    workflows.bindModules({
      'for-each': () => import('./pipelines/for-each')
    })

    const onPipelineB = jest.fn()
    workflows.events.on('b', onPipelineB)

    const forEachCount: ForEachCount = { count: 10 } 
    await workflows.run('for-each', 'a', { args: forEachCount })
    
    expect(onPipelineB).toHaveBeenCalledTimes(forEachCount.count)
  })
})