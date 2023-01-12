'use strict';

const e = React.createElement;
const opts = {auth: {username: "admin", password: "mariadb"}}

function countRows(str){
  return str.split('\n').length + 1
}

function countCols(str){
  var max_len = 30
  for (var v of str.split('\n')){
    if (v.length > max_len){
      max_len = v.length
    }
  }
  return max_len
}

class TextForm extends React.Component {
  constructor(props) {
    super(props);
    this.handleChange = this.handleChange.bind(this);
  }

  handleChange(event) {
    this.props.onChange(event.target.value)
  }
  
  render() {
    return e('form', {className: this.props.className},
             e('label', null, this.props.label),
             e('textarea', {value: this.props.value,
                            onChange: this.handleChange,
                            className: this.props.className,
                            rows: countRows(this.props.value),
                            cols: countCols(this.props.value)
                            })
            )
  }
}

class ETLTester extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      connection_string: 'DRIVER=Postgresql;SERVER=127.0.0.1;PORT=5432;UID=maxuser;PWD=maxpwd;DATABASE=demo;Protocol=7.4-0;UseDeclareFetch=1',
      json: '',
      response: '',
      status: 'Not connected',
      tables: [],
      target: 'server1',
      user: 'maxuser',
      password: 'maxpwd',
    };
  }

  render() {
    return e('div', null,
             e('pre', null, this.state.status),
             e('br'),
             e('button', {onClick: () => this.connect()}, 'Connect'),
             e('button', {onClick: () => this.prepare()}, 'Prepare'),
             e('button', {onClick: () => this.start()}, 'Start'),
             e('button', {onClick: () => this.disconnect()}, 'Disconnect'),
             e('div', null,
               e(TextForm, {label: 'Connection String', value: this.state.connection_string, onChange: (e) => this.updateValue('connection_string', e), className: 'input'}),
               e(TextForm, {label: 'Target', value: this.state.target, onChange: (e) => this.updateValue('target', e), className: 'input'}),
               e(TextForm, {label: 'User', value: this.state.user, onChange: (e) => this.updateValue('user', e), className: 'input'}),
               e(TextForm, {label: 'Password', value: this.state.password, onChange: (e) => this.updateValue('password', e), className: 'input'})
              ),
             e(TextForm, {label: 'Request', value: this.state.json, onChange: (e) => this.updateValue('json', e), className: 'requests'}),
             e(TextForm, {label: 'Response', value: this.state.response, onChange: (e) => this.updateValue('response', e), className: 'results'}),
             this.prettyTables()
            )
  }

  prettyTables(){
    var arr = []
    for (var t of this.state.tables) {
      if (t.create){
        arr.push(e('div', {key: t.schema + '.' + t.table},
                   e('h2', null, 'Table: ' + t.schema + '.' + t.table),
                   e('h3', null, 'Create'),
                   e('pre', null, t.create),
                   e('h3', null, 'Select'),
                   e('pre', null, t.select),
                   e('h3', null, 'Insert'),
                   e('pre', null, t.insert),
                  ))        
      }

    }
    return arr
  }
  
  async connect(){
    this.msg("Connecting")
    try
    {
    
    const payload = {
      target: "odbc",
      connection_string: this.state.connection_string
    }

    var res = await axios.post("http://localhost:8989/sql/", payload, opts)

    this.token= "?token=" + res.data.meta.token
    this.conn = res.data.links.self

    const second_payload = {
      target: this.state.target,
      user: this.state.user,
      password: this.state.password
    }
    
    res = await axios.post("http://localhost:8989/sql/", second_payload, opts)
    this.second_token = "?token=" + res.data.meta.token
    this.target_token = "&target_token=" + res.data.meta.token
    this.second_conn = res.data.links.self
    this.second_id = res.data.data.id

    this.msg("Connected")

    const v = {
      //type: "mariadb",
      type: "postgresql",
      //type: "generic",
      //catalog: "demo",
      target: this.second_id,
      tables: [{schema: "", table: ""}]
    }

      this.setState({json: JSON.stringify(v, null, 2)})
    } catch (err) {
      this.logError(err)
    }
  }

  logError(err){
    if (err.response){
      if (err.response.data) {
        this.msg(JSON.stringify(err.response.data, null, 2))
      } else{
        this.msg(JSON.stringify(err.response, null, 2))
      }
    } else {
      this.msg(JSON.stringify(err, null, 2))
    }
  }
  
  msg(txt){
    this.setState({status: txt})
  }

  async disconnect() {
    try {
      this.msg("Disconnecting")
      await axios.delete(this.second_conn + this.second_token, opts)
      await axios.delete(this.conn + this.token, opts)
      this.msg("Disconnected")
    } catch (err) {
      this.logError(err)
    }
  }
  
  async prepare() {
    try {
      this.msg("Preparing")
      var res = await axios.post(this.conn + "etl/prepare" + this.token + this.target_token, this.state.json, opts)
      const self = res.data.links.self

      while (res.status == 202)
      {
        await new Promise((res) => setTimeout(res, 1000));
        res = await axios.get(self + this.token, opts)
      }

      const js = JSON.parse(this.state.json)
      js.tables = res.data.data.attributes.results.tables
      this.setState({
        tables: js.tables,
        json: JSON.stringify(js, null, 2),
        response : JSON.stringify(res.data, null, 2)
      })
      
      this.msg("Prepared")
    } catch (err) {
      this.logError(err)
    }
  }
  
  async start(){
    try {
      this.msg("Starting")
      var res = await axios.post(this.conn + "etl/start" + this.token + this.target_token, this.state.json, opts)
      const self = res.data.links.self

      while (res.status == 202)
      {
        await new Promise((res) => setTimeout(res, 1000));
        res = await axios.get(self + this.token, opts)
      }

      this.setState({response: JSON.stringify(res.data, null, 2)})
      this.msg("Done")
    } catch (err) {
      this.logError(err)
    }
  }

  updateValue(name, val){
    const v = {};
    v[name] = val;
    this.setState(v)
  }
}

const domContainer = document.getElementById('root');
const root = ReactDOM.createRoot(domContainer);
root.render(e(ETLTester));
